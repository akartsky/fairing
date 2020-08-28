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

import time
logger = getLogger(__name__)

DEFAULT_LOGGING_INTERVAL_TIME_SEC = 30

class SageMakerJob(DeployerInterface):
    """Deploys a training job to the cluster"""
    def __init__(self, region=None, role=None, instance_count=None, instance_type=None, job_config={}, stream_logs=False):
        """
        :param region
        :param role
        :param job_config
        :param stream_logs
        """
        self._region = region
        self._role = role
        self._instance_count = instance_count
        self._instance_type = instance_type
        self._job_config = job_config
        self._stream_logs = stream_logs
        self._client = self.get_sagemaker_client(self._region)


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


    def create_request_dict(self, pod_template_spec):
        image_uri = pod_template_spec.containers[0].image
        self._job_name = f'fairingjob-{utils.random_tag()}'

        request = self._job_config
        request["TrainingJobName"] = self._job_name
        if 'AlgorithmSpecification' not in request:
            request['AlgorithmSpecification'] = {}

        request["AlgorithmSpecification"]["TrainingImage"] = image_uri

        if 'ResourceConfig' not in request:
            request['ResourceConfig'] = {}

        if self._instance_count:
            request['ResourceConfig']['InstanceCount'] = self._instance_count

        if self._instance_type:
            request['ResourceConfig']['InstanceType'] = self._instance_type

        if self._role:
            request["RoleArn"] = self._role


        return request

    def deploy(self, pod_template_spec):
        """Deploys the training job

        :param pod_template_spec: pod template spec

        """
        request = self.create_request_dict(pod_template_spec)

        try:
            logger.info(f'Creating training job with the following options: {request}')
            logger.info('Job submitted successfully.')
            self._client.create_training_job(**request)
            self.get_logs()
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])

    def get_logs(self):
        """Streams the logs for the training job"""
        logger.info('Access job logs at the following URL:')
        logger.info(f'https://console.aws.amazon.com/cloudwatch/home?region={self._region}#logStream:group=/aws/sagemaker/TrainingJobs;prefix={self._job_name}')
        if self._stream_logs:
            self.stream_logs()

    def stream_logs(self):
        try:
            self.wait_for_training_job()
        except:
            raise
        finally:
            cw_client = self.get_cloudwatch_client()
            self.print_logs_for_job(cw_client, '/aws/sagemaker/TrainingJobs')

    def print_logs_for_job(self, cw_client, log_grp):
        """Gets the CloudWatch logs for SageMaker jobs"""
        try:
            logger.info('\n******************** CloudWatch logs for {} {} ********************\n'.format(log_grp, self._job_name))

            log_streams = cw_client.describe_log_streams(
                logGroupName=log_grp,
                logStreamNamePrefix=self._job_name + '/'
            )['logStreams']

            for log_stream in log_streams:
                logger.info('\n***** {} *****\n'.format(log_stream['logStreamName']))
                response = cw_client.get_log_events(
                    logGroupName=log_grp,
                    logStreamName=log_stream['logStreamName']
                )
                for event in response['events']:
                    logger.info(event['message'])

            logger.info('\n******************** End of CloudWatch logs for {} {} ********************\n'.format(log_grp, self._job_name))
        except Exception as e:
            logger.error(CW_ERROR_MESSAGE)
            logger.error(e)


    def wait_for_training_job(self, poll_interval=30):
        while(True):
            response = self._client.describe_training_job(TrainingJobName=self._job_name)
            status = response['TrainingJobStatus']
            if status == 'Completed':
                logger.info("Training job ended with status: " + status)
                break
            if status == 'Failed':
                message = response['FailureReason']
                logger.info(f'Training failed with the following error: {message}')
                raise Exception('Training job failed')
            logger.info("Training job is still in status: " + status)
            time.sleep(poll_interval)

    def get_cloudwatch_client(self):
        session = self.get_boto3_session(self._region)
        client = session.client('logs')
        return client
