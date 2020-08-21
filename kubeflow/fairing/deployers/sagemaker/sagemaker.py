from kubeflow.fairing import utils

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
    def __init__(self):
        return

    def get_boto3_session(self, region, role_arn=None):
        """Creates a boto3 session, optionally assuming a role"""

        # By default return a basic session
        if role_arn is None:
            return Session(region_name=region)

        # The following assume role example was taken from
        # https://github.com/boto/botocore/issues/761#issuecomment-426037853

        # Create a session used to assume role
        assume_session = BotocoreSession()
        fetcher = AssumeRoleCredentialFetcher(
            assume_session.create_client,
            assume_session.get_credentials(),
            role_arn,
            extra_args={
                'DurationSeconds': 3600, # 1 hour assume assume by default
            },
            cache=JSONFileCache()
        )
        role_session = BotocoreSession()
        role_session.register_component(
            'credential_provider',
            CredentialResolver([AssumeRoleProvider(fetcher)])
        )
        return Session(region_name=region, botocore_session=role_session)
    def get_sagemaker_client(self, region):
        session = self.get_boto3_session(region)
        client = session.client('sagemaker')
        return client

 
    def deploy(self, pod_template_spec):
        """Deploys the training job

        :param pod_template_spec: pod template spec

        """

        image_uri = pod_template_spec.containers[0].image
        print(image_uri)
        job_name = f"fairingjob-{utils.random_tag()}"
        print(job_name)
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

        client.create_training_job(**request)

    def get_logs(self):
        """Streams the logs for the training job"""
        raise NotImplementedError('TrainingInterface.train')
