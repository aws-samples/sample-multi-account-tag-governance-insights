import hashlib
import importlib.util
import json
import subprocess
import sys
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from deployment import package_lambdas
from deployment.package_lambdas import LambdaPackager, sign_lambda_packages


def make_minimal_project(tmp_path):
    project = tmp_path / "project"
    cost_dir = project / "src" / "lambda_handlers" / "cost"
    cost_dir.mkdir(parents=True)
    (project / "deployment").mkdir()
    (project / "requirements.txt").write_text("", encoding="utf-8")
    (project / "deployment" / "requirements-lambda.txt").write_text(
        "",
        encoding="utf-8",
    )
    (project / "src" / "__init__.py").write_text("", encoding="utf-8")
    (project / "src" / "lambda_handlers" / "__init__.py").write_text(
        "",
        encoding="utf-8",
    )
    (cost_dir / "__init__.py").write_text("", encoding="utf-8")
    (cost_dir / "query_builder.py").write_text("VALUE = 1\n", encoding="utf-8")
    (cost_dir / "cost_analyzer_worker_handler.py").write_text(
        "def lambda_handler(event, context):\n    return {}\n",
        encoding="utf-8",
    )
    return project


def test_worker_only_selection_does_not_build_other_functions(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    packager.package_lambda_function = Mock(
        return_value=str(packager.build_dir / "cost_analyzer_worker.zip")
    )

    paths = packager.package_lambdas(["cost_analyzer_worker"])

    assert list(paths) == ["cost_analyzer_worker"]
    packager.package_lambda_function.assert_called_once()
    assert (
        packager.package_lambda_function.call_args.kwargs["function_name"]
        == "cost_analyzer_worker"
    )


def test_function_registry_lists_all_lambda_functions():
    functions = LambdaPackager._lambda_functions()

    assert set(functions) == {
        "aggregator_coordinator",
        "aggregator_worker",
        "analytics_writer",
        "monthly_aggregator",
        "cost_analyzer_coordinator",
        "cost_analyzer_worker",
        "cost_deduplicator",
    }


def test_unknown_package_selection_is_rejected(tmp_path):
    packager = LambdaPackager(str(make_minimal_project(tmp_path)))

    with pytest.raises(ValueError, match="Unknown Lambda function"):
        packager.package_lambdas(["not-a-function"])


def test_local_worker_package_contains_v4_cost_path_only(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    packager._install_dependencies = Mock()

    paths = packager.package_lambdas(["cost_analyzer_worker"])

    with zipfile.ZipFile(paths["cost_analyzer_worker"]) as package:
        names = package.namelist()
    assert "src/lambda_handlers/cost/query_builder.py" in names
    assert not any(name.startswith("src/cost/") for name in names)
    assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)


def test_nested_handler_path_generates_dotted_import(tmp_path):
    packager = LambdaPackager(str(make_minimal_project(tmp_path)))
    staging = tmp_path / "staging"
    staging.mkdir()

    packager._create_index_file(
        staging,
        "cost/cost_analyzer_worker_handler.py",
    )

    assert (
        "from cost.cost_analyzer_worker_handler import lambda_handler"
        in (staging / "index.py").read_text(encoding="utf-8")
    )


def test_top_level_handler_import_remains_unchanged(tmp_path):
    packager = LambdaPackager(str(make_minimal_project(tmp_path)))
    staging = tmp_path / "staging"
    staging.mkdir()

    packager._create_index_file(staging, "aggregator_worker.py")

    assert (
        "from aggregator_worker import lambda_handler"
        in (staging / "index.py").read_text(encoding="utf-8")
    )


def test_extracted_worker_zip_loads_index_handler_without_root_copy(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    packager._install_dependencies = Mock()

    paths = packager.package_lambdas(["cost_analyzer_worker"])

    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(paths["cost_analyzer_worker"]) as package:
        names = package.namelist()
        package.extractall(extracted)

    assert "cost/cost_analyzer_worker_handler.py" in names
    assert "cost_analyzer_worker_handler.py" not in names

    spec = importlib.util.spec_from_file_location(
        "packaged_worker_index",
        extracted / "index.py",
    )
    module = importlib.util.module_from_spec(spec)
    with patch.object(sys, "path", [str(extracted), *sys.path]):
        spec.loader.exec_module(module)

    assert module.lambda_handler({}, None) == {}


def test_dependency_install_failure_leaves_no_accepted_zip(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package_path = packager.build_dir / "cost_analyzer_worker.zip"
    package_path.write_bytes(b"stale-package")

    with (
        patch(
            "deployment.package_lambdas.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, ["pip"]),
        ),
        patch.object(packager, "_create_zip_package") as create_zip,
        patch.object(
            packager,
            "_validate_package",
            return_value={"size_mb": 0, "file_count": 0},
        ) as validate_package,
        pytest.raises(subprocess.CalledProcessError),
    ):
        packager.package_lambda_function(
            function_name="cost_analyzer_worker",
            handler_file="cost/cost_analyzer_worker_handler.py",
        )

    assert not package_path.exists()
    create_zip.assert_not_called()
    validate_package.assert_not_called()


def test_dependency_install_targets_python311_manylinux_x86_64(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    staging = tmp_path / "staging"
    staging.mkdir()

    with patch("deployment.package_lambdas.subprocess.run") as run:
        packager._install_dependencies(staging)

    command = run.call_args.args[0]
    assert command[command.index("--platform") + 1] == "manylinux2014_x86_64"
    assert command[command.index("--python-version") + 1] == "3.11"
    assert command[command.index("--implementation") + 1] == "cp"
    assert command[command.index("--abi") + 1] == "cp311"
    assert command[command.index("--only-binary") + 1] == ":all:"
    assert run.call_args.kwargs["check"] is True


def test_generated_bytecode_is_removed_before_packaging(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    staging = tmp_path / "staging"
    cache = staging / "dependency" / "__pycache__"
    cache.mkdir(parents=True)
    (cache / "module.cpython-311.pyc").write_bytes(b"bytecode")
    (staging / "loose.pyc").write_bytes(b"bytecode")

    packager._remove_generated_bytecode(staging)

    assert not cache.exists()
    assert not (staging / "loose.pyc").exists()


def test_manifest_binds_source_tree_zip_hash_file_count_and_intended_key(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package = packager.build_dir / "cost_analyzer_worker.zip"
    with zipfile.ZipFile(package, "w") as archive:
        archive.writestr("index.py", "VALUE = 1\n")
    package_sha256 = hashlib.sha256(package.read_bytes()).hexdigest()

    manifest_path = packager.create_deployment_manifest(
        {"cost_analyzer_worker": str(package)},
        deployment_version="20260821-cur2",
        source_commit="cf8910b3b6bdc29ab007052e8308330b1aa35ca7",
        source_tree="0123456789abcdef0123456789abcdef01234567",
    )
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    worker = manifest["packages"]["cost_analyzer_worker"]

    assert manifest["source_commit"].startswith("cf8910b")
    assert manifest["source_tree"] == "0123456789abcdef0123456789abcdef01234567"
    assert worker["sha256"] == package_sha256
    assert worker["file_count"] == 1
    assert worker["intended_s3_key"] == (
        "lambda-packages/v20260821-cur2/cost_analyzer_worker.zip"
    )


@patch("boto3.client")
def test_versioned_only_upload_never_writes_latest_and_sets_metadata(
    mock_boto_client,
    tmp_path,
):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package = packager.build_dir / "cost_analyzer_worker.zip"
    package.write_bytes(b"worker-package")
    s3 = mock_boto_client.return_value

    urls = packager.upload_packages_to_s3(
        {"cost_analyzer_worker": str(package)},
        "artifact-bucket",
        deployment_version="20260821-cur2",
        versioned_only=True,
        source_commit="cf8910b3b6bdc29ab007052e8308330b1aa35ca7",
        source_tree="0123456789abcdef0123456789abcdef01234567",
    )

    s3.put_object.assert_called_once()
    put_args = s3.put_object.call_args.kwargs
    assert put_args["Bucket"] == "artifact-bucket"
    assert put_args["Key"] == (
        "lambda-packages/v20260821-cur2/cost_analyzer_worker.zip"
    )
    assert put_args["IfNoneMatch"] == "*"
    assert put_args["ServerSideEncryption"] == "AES256"
    assert put_args["Metadata"]["source-commit"].startswith("cf8910b")
    assert put_args["Metadata"]["source-tree"] == (
        "0123456789abcdef0123456789abcdef01234567"
    )
    assert put_args["Metadata"]["sha256"] == hashlib.sha256(
        b"worker-package"
    ).hexdigest()
    assert "lambda-packages/cost_analyzer_worker.zip" not in str(
        s3.put_object.call_args_list
    )
    assert "v20260821-cur2" in urls["cost_analyzer_worker"]


def test_versioned_only_upload_requires_explicit_version(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package = packager.build_dir / "cost_analyzer_worker.zip"
    package.write_bytes(b"worker-package")

    with pytest.raises(ValueError, match="deployment_version"):
        packager.upload_packages_to_s3(
            {"cost_analyzer_worker": str(package)},
            "artifact-bucket",
            versioned_only=True,
            source_commit="cf8910b",
            source_tree="0123456789abcdef0123456789abcdef01234567",
        )


def test_upload_requires_source_tree(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package = packager.build_dir / "cost_analyzer_worker.zip"
    package.write_bytes(b"worker-package")

    with pytest.raises(ValueError, match="source_tree"):
        packager.upload_packages_to_s3(
            {"cost_analyzer_worker": str(package)},
            "artifact-bucket",
            deployment_version="20260821-cur2",
            versioned_only=True,
            source_commit="cf8910b",
        )


def test_cli_rejects_mutable_upload_mode(monkeypatch, tmp_path):
    project = make_minimal_project(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "package_lambdas.py",
            "--project-root",
            str(project),
            "--version",
            "20260825-source-bound",
            "--upload-bucket",
            "artifact-bucket",
        ],
    )

    with pytest.raises(SystemExit) as error:
        package_lambdas.main()

    assert error.value.code == 1


@patch("boto3.client")
def test_production_signing_uses_versioned_source_and_destination(
    mock_boto_client,
):
    signer = Mock()
    s3 = Mock()
    mock_boto_client.side_effect = [signer, s3]
    signer.start_signing_job.return_value = {"jobId": "job-1"}
    signer.describe_signing_job.return_value = {
        "status": "Succeeded",
        "signedObject": {
            "s3": {
                "key": "signed/v20260825-source-bound/aggregator_worker.zip",
            },
        },
    }

    signed = sign_lambda_packages(
        {"aggregator_worker": "/tmp/aggregator_worker.zip"},
        "artifact-bucket",
        deployment_version="20260825-source-bound",
    )

    request = signer.start_signing_job.call_args.kwargs
    assert request["source"]["s3"]["key"] == (
        "lambda-packages/v20260825-source-bound/aggregator_worker.zip"
    )
    assert request["destination"]["s3"]["prefix"] == (
        "signed/v20260825-source-bound/"
    )
    assert signed == {
        "aggregator_worker": (
            "signed/v20260825-source-bound/aggregator_worker.zip"
        ),
    }
    s3.copy_object.assert_not_called()
    s3.delete_object.assert_not_called()
