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
    (cost_dir / "cost_allocator_handler.py").write_text(
        "def lambda_handler(event, context):\n    return {}\n",
        encoding="utf-8",
    )
    return project


def test_worker_only_selection_does_not_build_other_functions(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    packager.package_lambda_function = Mock(
        return_value=str(packager.build_dir / "cost_allocator.zip")
    )

    paths = packager.package_lambdas(["cost_allocator"])

    assert list(paths) == ["cost_allocator"]
    packager.package_lambda_function.assert_called_once()
    assert (
        packager.package_lambda_function.call_args.kwargs["function_name"]
        == "cost_allocator"
    )


def test_function_registry_lists_all_lambda_functions():
    functions = LambdaPackager._lambda_functions()

    assert "include_dirs" not in functions["analytics_writer"]
    assert set(functions) == {
        "aggregator_coordinator",
        "aggregator_worker",
        "analytics_writer",
        "monthly_aggregator",
        "cost_allocator",
        "tag_compliance_rule",
        "tag_compliance_report",
    }


def test_unknown_package_selection_is_rejected(tmp_path):
    packager = LambdaPackager(str(make_minimal_project(tmp_path)))

    with pytest.raises(ValueError, match="Unknown Lambda function"):
        packager.package_lambdas(["not-a-function"])


def test_local_worker_package_contains_v4_cost_path_only(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    packager._install_dependencies = Mock()

    paths = packager.package_lambdas(["cost_allocator"])

    with zipfile.ZipFile(paths["cost_allocator"]) as package:
        names = package.namelist()
    assert "cost/query_builder.py" in names
    assert not any(name.startswith("src/") for name in names)
    assert "requirements.txt" not in names
    assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)


def test_analytics_writer_zip_ships_delta_schema_next_to_handler(tmp_path):
    project = make_minimal_project(tmp_path)
    (project / "src" / "lambda_handlers" / "analytics_writer_handler.py").write_text(
        "def lambda_handler(event, context):\n    return {}\n", encoding="utf-8")
    (project / "src" / "schemas").mkdir()
    (project / "src" / "schemas" / "delta_schema.json").write_text('{"type": "object"}', encoding="utf-8")
    (project / "src" / "tests").mkdir()
    (project / "src" / "tests" / "test_x.py").write_text("", encoding="utf-8")
    packager = LambdaPackager(str(project))
    packager._install_dependencies = Mock()

    paths = packager.package_lambdas(["analytics_writer"])

    with zipfile.ZipFile(paths["analytics_writer"]) as package:
        names = package.namelist()
        assert package.read("schemas/delta_schema.json") == b'{"type": "object"}'
    assert "analytics_writer_handler.py" in names
    assert not any("tests/" in name for name in names)


def test_missing_data_file_fails_packaging(tmp_path):
    project = make_minimal_project(tmp_path)
    (project / "src" / "lambda_handlers" / "analytics_writer_handler.py").write_text("", encoding="utf-8")
    packager = LambdaPackager(str(project))
    packager._install_dependencies = Mock()

    with pytest.raises(FileNotFoundError, match="delta_schema.json"):
        packager.package_lambdas(["analytics_writer"])


def test_nested_handler_path_generates_dotted_import(tmp_path):
    packager = LambdaPackager(str(make_minimal_project(tmp_path)))
    staging = tmp_path / "staging"
    staging.mkdir()

    packager._create_index_file(
        staging,
        "cost/cost_allocator_handler.py",
    )

    assert (
        "from cost.cost_allocator_handler import lambda_handler"
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

    paths = packager.package_lambdas(["cost_allocator"])

    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(paths["cost_allocator"]) as package:
        names = package.namelist()
        package.extractall(extracted)

    assert "cost/cost_allocator_handler.py" in names
    assert "cost_allocator_handler.py" not in names

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
    package_path = packager.build_dir / "cost_allocator.zip"
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
            function_name="cost_allocator",
            handler_file="cost/cost_allocator_handler.py",
        )

    assert not package_path.exists()
    create_zip.assert_not_called()
    validate_package.assert_not_called()


def test_dependency_install_targets_python314_manylinux_x86_64(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    staging = tmp_path / "staging"
    staging.mkdir()

    with patch("deployment.package_lambdas.subprocess.run") as run:
        packager._install_dependencies(staging)

    command = run.call_args.args[0]
    assert command[command.index("--platform") + 1] == "manylinux2014_x86_64"
    assert command[command.index("--python-version") + 1] == "3.14"
    assert command[command.index("--implementation") + 1] == "cp"
    assert command[command.index("--abi") + 1] == "cp314"
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
    package = packager.build_dir / "cost_allocator.zip"
    with zipfile.ZipFile(package, "w") as archive:
        archive.writestr("index.py", "VALUE = 1\n")
    package_sha256 = hashlib.sha256(package.read_bytes()).hexdigest()

    manifest_path = packager.create_deployment_manifest(
        {"cost_allocator": str(package)},
        deployment_version="20260821-cur2",
        source_commit="cf8910b3b6bdc29ab007052e8308330b1aa35ca7",
        source_tree="0123456789abcdef0123456789abcdef01234567",
    )
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    worker = manifest["packages"]["cost_allocator"]

    assert manifest["source_commit"].startswith("cf8910b")
    assert manifest["source_tree"] == "0123456789abcdef0123456789abcdef01234567"
    assert worker["sha256"] == package_sha256
    assert worker["file_count"] == 1
    assert worker["intended_s3_key"] == (
        "lambda-packages/v20260821-cur2/cost_allocator.zip"
    )


@patch("boto3.client")
def test_versioned_only_upload_never_writes_latest_and_sets_metadata(
    mock_boto_client,
    tmp_path,
):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package = packager.build_dir / "cost_allocator.zip"
    package.write_bytes(b"worker-package")
    s3 = mock_boto_client.return_value

    urls = packager.upload_packages_to_s3(
        {"cost_allocator": str(package)},
        "amzn-s3-demo-artifact-bucket",
        deployment_version="20260821-cur2",
        versioned_only=True,
        source_commit="cf8910b3b6bdc29ab007052e8308330b1aa35ca7",
        source_tree="0123456789abcdef0123456789abcdef01234567",
    )

    s3.put_object.assert_called_once()
    put_args = s3.put_object.call_args.kwargs
    assert put_args["Bucket"] == "amzn-s3-demo-artifact-bucket"
    assert put_args["Key"] == (
        "lambda-packages/v20260821-cur2/cost_allocator.zip"
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
    assert "lambda-packages/cost_allocator.zip" not in str(
        s3.put_object.call_args_list
    )
    assert "v20260821-cur2" in urls["cost_allocator"]


def test_versioned_only_upload_requires_explicit_version(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package = packager.build_dir / "cost_allocator.zip"
    package.write_bytes(b"worker-package")

    with pytest.raises(ValueError, match="deployment_version"):
        packager.upload_packages_to_s3(
            {"cost_allocator": str(package)},
            "amzn-s3-demo-artifact-bucket",
            versioned_only=True,
            source_commit="cf8910b",
            source_tree="0123456789abcdef0123456789abcdef01234567",
        )


def test_upload_requires_source_tree(tmp_path):
    project = make_minimal_project(tmp_path)
    packager = LambdaPackager(str(project))
    package = packager.build_dir / "cost_allocator.zip"
    package.write_bytes(b"worker-package")

    with pytest.raises(ValueError, match="source_tree"):
        packager.upload_packages_to_s3(
            {"cost_allocator": str(package)},
            "amzn-s3-demo-artifact-bucket",
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
            "amzn-s3-demo-artifact-bucket",
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
        "amzn-s3-demo-artifact-bucket",
        deployment_version="20260825-source-bound",
        source_version_ids={"aggregator_worker": "3HL4kqtJlcpXroDTDmJ"},
    )

    request = signer.start_signing_job.call_args.kwargs
    assert request["source"]["s3"]["key"] == (
        "lambda-packages/v20260825-source-bound/aggregator_worker.zip"
    )
    assert request["source"]["s3"]["version"] == "3HL4kqtJlcpXroDTDmJ"
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


@pytest.mark.parametrize("version_id", [None, "", "null"])
@patch("boto3.client")
def test_signing_requires_versioned_bucket(mock_boto_client, version_id):
    with pytest.raises(ValueError, match="enable versioning"):
        sign_lambda_packages(
            {"aggregator_worker": "/tmp/aggregator_worker.zip"},
            "amzn-s3-demo-artifact-bucket",
            deployment_version="20260825-source-bound",
            source_version_ids={"aggregator_worker": version_id},
        )
    mock_boto_client.return_value.start_signing_job.assert_not_called()


@patch("boto3.client")
def test_upload_records_version_ids(mock_boto_client, tmp_path):
    packager = LambdaPackager(str(make_minimal_project(tmp_path)))
    package = packager.build_dir / "cost_allocator.zip"
    package.write_bytes(b"worker-package")
    mock_boto_client.return_value.put_object.return_value = {"VersionId": "v-123"}

    packager.upload_packages_to_s3(
        {"cost_allocator": str(package)}, "amzn-s3-demo-artifact-bucket",
        deployment_version="1", versioned_only=True, source_commit="c", source_tree="t",
    )

    assert packager.uploaded_version_ids == {"cost_allocator": "v-123"}


def test_prod_environment_does_not_sign(monkeypatch, tmp_path):
    project = make_minimal_project(tmp_path)
    monkeypatch.setattr(sys, "argv", [
        "package_lambdas.py", "--project-root", str(project), "--version", "1",
        "--environment", "prod", "--upload-bucket", "amzn-s3-demo-artifact-bucket", "--versioned-only",
        "--source-commit", "c", "--source-tree", "t",
    ])
    with patch.object(LambdaPackager, "package_lambdas", return_value={}), \
            patch.object(LambdaPackager, "upload_packages_to_s3", return_value={}), \
            patch.object(LambdaPackager, "create_deployment_manifest", return_value="m.json"), \
            patch.object(package_lambdas, "sign_lambda_packages") as sign:
        package_lambdas.main()
    sign.assert_not_called()


def test_sign_flag_signs_with_uploaded_versions(monkeypatch, tmp_path):
    project = make_minimal_project(tmp_path)
    monkeypatch.setattr(sys, "argv", [
        "package_lambdas.py", "--project-root", str(project), "--version", "1", "--sign",
        "--upload-bucket", "amzn-s3-demo-artifact-bucket", "--versioned-only",
        "--source-commit", "c", "--source-tree", "t",
    ])

    def upload(self, *a, **k):
        self.uploaded_version_ids = {"cost_allocator": "v-1"}
        return {}

    with patch.object(LambdaPackager, "package_lambdas", return_value={"cost_allocator": "x.zip"}), \
            patch.object(LambdaPackager, "upload_packages_to_s3", upload), \
            patch.object(LambdaPackager, "create_deployment_manifest", return_value="m.json"), \
            patch.object(package_lambdas, "sign_lambda_packages", return_value={}) as sign:
        package_lambdas.main()
    assert sign.call_args.kwargs["source_version_ids"] == {"cost_allocator": "v-1"}


def test_sign_without_upload_bucket_rejected(monkeypatch, tmp_path):
    project = make_minimal_project(tmp_path)
    monkeypatch.setattr(sys, "argv", ["package_lambdas.py", "--project-root", str(project),
                                      "--version", "1", "--sign"])
    with pytest.raises(SystemExit):
        package_lambdas.main()


def test_run_argv_refuses_shell_and_strings():
    import pytest
    from deployment.package_lambdas import run_argv
    with pytest.raises(ValueError):
        run_argv("pip install x")
    with pytest.raises(ValueError):
        run_argv(["pip", "install", "x"], **{"shell": True})  # must be refused


def test_zip_entries_are_world_readable_under_restrictive_umask(tmp_path):
    # Lambda runs the code as a different user than the zip's file owner: a
    # file packed as 0600/0640 (umask 077/027 build hosts) fails to import.
    staging = tmp_path / "staging"
    (staging / "pkg").mkdir(parents=True)
    handler = staging / "index.py"
    handler.write_text("def lambda_handler(event, context):\n    return {}\n", encoding="utf-8")
    handler.chmod(0o600)
    native = staging / "pkg" / "_speedups.so"
    native.write_bytes(b"\x7fELF")
    native.chmod(0o700)
    project = make_minimal_project(tmp_path)
    package = tmp_path / "out.zip"

    LambdaPackager(str(project))._create_zip_package(staging, package)

    with zipfile.ZipFile(package) as archive:
        modes = {i.filename: (i.external_attr >> 16) & 0o777 for i in archive.infolist()}
        assert archive.read("index.py").startswith(b"def lambda_handler")
        assert all(i.compress_type == zipfile.ZIP_DEFLATED for i in archive.infolist())
    assert modes == {"index.py": 0o644, "pkg/_speedups.so": 0o755}
