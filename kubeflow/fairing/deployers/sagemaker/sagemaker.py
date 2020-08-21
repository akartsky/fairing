from kubeflow.fairing import utils

from sagemaker.estimator import Estimator

from botocore.exceptions import ClientError
from botocore.session import Session as BotocoreSession



from logging import getLogger
from kubeflow.fairing.deployers.deployer import DeployerInterface
from boto3.session import Session
from botocore.session import Session as BotocoreSession
from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache
)

logger = getLogger(__name__)

class SageMakerJob(DeployerInterface):
    """Deploys a training job to the cluster"""
    def __init__(self, role, instance_count, instance_type, stream_logs):
        self.role = role
        self.instance_count = instance_count
        self.instance_type = instance_type
        self.stream_logs = stream_logs

    def deploy(self, pod_template_spec):
        """Deploys the training job

        :param pod_template_spec: pod template spec

        """

        image_uri = pod_template_spec.containers[0].image
        job_name = f"fairingjob-{utils.random_tag()}"
        estimator = Estimator(image_uri, "ml.m4.xlarge", base_job_name=job_name)

        estimator.fit()
        """
        region = "us-east-1"
        client = self.get_sagemaker_client(region)
        request = {}
        request["AlgorithmSpecification"] = {}
        request["TrainingJobName"] = job_name
        request["AlgorithmSpecification"]["TrainingImage"] = image_uri
        request["AlgorithmSpecification"]["TrainingInputMode"] = "File"
        request["OutputDataConfig"] = {}
        request["OutputDataConfig"]["S3OutputPath"] = "s3://dusluong-bucket0/fairing/output"
        request["ResourceConfig"] = {}
        request["ResourceConfig"]["InstanceCount"] = 1
        request["ResourceConfig"]["InstanceType"] = "ml.m4.xlarge"
        request["ResourceConfig"]["VolumeSizeInGB"] = 1
        request["RoleArn"] = "arn:aws:iam::169544399729:role/kfp-example-sagemaker-execution-role"
        request["StoppingCondition"] = {"MaxRuntimeInSeconds": 3600}
        """

        #client.create_training_job(**request)

    def get_logs(self):
        """Streams the logs for the training job"""
        print("Getting logs")

        raise NotImplementedError('TrainingInterface.train')
