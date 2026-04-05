from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INFRASTRUCTURE_TRAINING_PATH = REPOSITORY_ROOT / "infrastructure" / "training.py"


def load_infrastructure_training() -> str:
    return INFRASTRUCTURE_TRAINING_PATH.read_text(encoding="utf-8")


def test_models_cluster_uses_ec2_backed_capacity_provider() -> None:
    infrastructure_training = load_infrastructure_training()

    assert '"models_cluster"' in infrastructure_training
    assert '"models_capacity_provider"' in infrastructure_training
    assert '"models_cluster_capacity_providers"' in infrastructure_training


def test_trainer_uses_gpu_instance_type() -> None:
    infrastructure_training = load_infrastructure_training()

    assert 'instance_type="g4dn.xlarge"' in infrastructure_training
    assert "amzn2-ami-ecs-gpu-hvm-*-x86_64-ebs" in infrastructure_training


def test_trainer_asg_scales_to_zero() -> None:
    infrastructure_training = load_infrastructure_training()

    assert "min_size=0" in infrastructure_training
    assert "desired_capacity=0" in infrastructure_training


def test_models_instance_profile_configured() -> None:
    infrastructure_training = load_infrastructure_training()

    assert '"models_instance_profile"' in infrastructure_training
    assert "AmazonEC2ContainerServiceforEC2Role" in infrastructure_training


def test_trainer_gpu_support_enabled_in_user_data() -> None:
    infrastructure_training = load_infrastructure_training()

    assert "ECS_ENABLE_GPU_SUPPORT=true" in infrastructure_training
