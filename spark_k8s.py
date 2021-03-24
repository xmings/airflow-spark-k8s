import time
import yaml
import logging
from kubernetes import config as kube_config
from kubernetes import client as kube_client
from kubernetes.client.rest import ApiException
from airflow.exceptions import AirflowException


K8S_CONF_FILE = "k8s.config"
K8S_RESOURCE_GROUP = "sparkoperator.k8s.io"
K8S_RESOURCE_VERSION = "v1beta2"
K8S_RESOURCE_PLURAL = "sparkapplications"
K8S_SPARK_NAMESPACE = "spark-operator"

K8S_APPLICATION_CONF_TEMPLATE = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: "{app_name}"
  namespace: {namespace}
spec:
  type: Scala
  mode: cluster
  image: "registry-vpc.cn-hangzhou.aliyuncs.com/afanticar/spark-merge:dev-latest"
  imagePullSecrets:
    - docker-register-secret
  imagePullPolicy: Always
  mainClass: {main_class}
  mainApplicationFile: "{jar_path}"
  sparkVersion: "{spark_version}"
  restartPolicy:
    type: Never
  driver:
    cores: {driver_cores}
    coreLimit: "{driver_core_limit}"
    memory: "{driver_memory}"
    memoryOverhead: "{driver_memory_overhead}"
    labels:
      version: 2.4.5
    serviceAccount: spark
  executor:
    cores: {executor_cores}
    memory: "{executor_memory}"
    instances: {executor_instances}
    labels:
      version: 2.4.5
"""

K8S_APPLICATION_CONF_KWARGS_DEFAULT = {
    "app_name": "Spark-Batch",
    "namespace": K8S_SPARK_NAMESPACE,
    "spark_version": "2.4.5",
    "driver_cores": 1,
    "driver_core_limit": 2,
    "driver_memory": "1g",
    "driver_memory_overhead": "1g",
    "executor_cores": 1,
    "executor_memory": "1g",
    "executor_instances": 2
}


class SparkOnK8s(object):
    def __init__(self, jar_path,
                 namespace=K8S_SPARK_NAMESPACE,
                 k8s_conf_file=K8S_CONF_FILE):
        self.jar_path = jar_path
        self.namespace = namespace
        self.logger = logging.getLogger(self.__class__.__name__)
        kube_config.load_kube_config(config_file=k8s_conf_file)
        self.custom_obj_api = kube_client.CustomObjectsApi(kube_client.ApiClient())
        self.core_v1_api = kube_client.CoreV1Api(kube_client.ApiClient())
        self.batch_v1_api = kube_client.BatchV1Api(kube_client.ApiClient())
        self.poll_interval = 30

    def run_spark_on_k8s(self, main_class, other_app_options: dict = None, timeout=3600):
        if "namespace" not in other_app_options:
            other_app_options.update({"namespace": self.namespace})

        app_name = main_class.split('.')[-1]
        app_options = K8S_APPLICATION_CONF_KWARGS_DEFAULT.copy()
        app_options.update(other_app_options)
        app_options.update({"jar_path": self.jar_path, "main_class": main_class, "app_name": app_name})
        app_conf = _load_body_to_dict(K8S_APPLICATION_CONF_TEMPLATE.format(app_options))

        self.custom_obj_api.create_namespaced_custom_object(
            group=K8S_RESOURCE_GROUP,
            version=K8S_RESOURCE_VERSION,
            namespace=self.namespace,
            plural=K8S_RESOURCE_PLURAL,
            body=app_conf,
            pretty=True
        )

        self.logger.info("Spark submit application: %s", main_class)

        self.wait(app_name=app_name, timeout=timeout)

    def wait(self, app_name, timeout):
        FAILURE_STATES = ("FAILED", "UNKNOWN")
        SUCCESS_STATES = ("COMPLETED",)
        start = int(time.time())

        while int(time.time()) - start < timeout:
            response = self.custom_obj_api.get_namespaced_custom_object_status(
                group=K8S_RESOURCE_GROUP,
                version=K8S_RESOURCE_VERSION,
                namespace=self.namespace,
                plural=K8S_RESOURCE_PLURAL,
                name=app_name
            )
            app_state = response["status"]["applicationState"]["state"]
            if app_state in FAILURE_STATES + SUCCESS_STATES:
                if app_state in FAILURE_STATES + SUCCESS_STATES:
                    pod_name = response["status"]["driverInfo"]["podName"]
                    namespace = response["metadata"]["namespace"]
                    try:
                        msg = self.core_v1_api.read_namespaced_pod_log(
                            name=pod_name, namespace=namespace, pretty=True)
                        if app_state in FAILURE_STATES:
                            self.logger.error(msg)
                        else:
                            self.logger.info(msg)
                    except ApiException as e:
                        self.logger.warning(
                            "Could not read logs for pod %s. It may have been disposed.\n"
                            "Make sure timeToLiveSeconds is set on your SparkApplication spec.\n"
                            "underlying exception: %s",
                            pod_name,
                            e,
                        )
                    if app_state in FAILURE_STATES:
                        raise AirflowException(f"Spark application failed with state: {app_state}")
                    elif app_state in SUCCESS_STATES:
                        self.logger.info("Spark application ended successfully")
                else:
                    self.logger.info("Spark application is still in state: %s", app_state)
            time.sleep(self.poll_interval)


def _load_body_to_dict(body):
    try:
        body_dict = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise AirflowException(f"Exception when loading resource definition: {e}\n")
    return body_dict


__all__ = ("SparkOnK8s", )
