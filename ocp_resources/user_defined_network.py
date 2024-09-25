from typing import Dict, Any, Optional
from kubernetes.dynamic import DynamicClient
from timeout_sampler import TimeoutSampler, TimeoutExpiredError
from ocp_resources.resource import NamespacedResource


class WaitForStatusConditionFailed(Exception):
    """Exception raised when waiting for a status condition fails."""

    pass


class UserDefinedNetwork(NamespacedResource):
    """
    UserDefinedNetwork object.

    API reference:
    https://ovn-kubernetes.io/api-reference/userdefinednetwork-api-spec/
    """

    api_group = NamespacedResource.ApiGroup.K8S_OVN_ORG

    def __init__(
        self,
        name: str,
        namespace: str,
        client: Optional[DynamicClient] = None,
        topology: Optional[str] = None,
        layer2: Optional[Dict[str, Any]] = None,
        layer3: Optional[Dict[str, Any]] = None,
        local_net: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): The namespace of the UserDefinedNetwork.
            client (Optional[DynamicClient]): DynamicClient to use.
            topology (Optional[str]): Topology describes network configuration.
            layer2 (Optional[Dict[str, Any]]): Layer2 is the Layer2 topology configuration.
            layer3 (Optional[Dict[str, Any]]): Layer3 is the Layer3 topology configuration.
            local_net (Optional[Dict[str, Any]]): LocalNet is the LocalNet topology configuration.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            **kwargs,
        )
        self.topology = topology
        self.layer2 = layer2
        self.layer3 = layer3
        self.local_net = local_net

    def to_dict(self) -> None:
        super().to_dict()
        if not self.yaml_file:
            self.res["spec"] = {}

            attributes = {
                "topology": self.topology,
                "layer2": self.layer2,
                "layer3": self.layer3,
                "localNet": self.local_net,
            }

            for key, value in attributes.items():
                if value is not None:
                    self.res["spec"][key] = value

    class Status(NamespacedResource.Condition):
        """
        This class represents the status conditions of a UserDefinedNetwork and provides specific
        status types and reasons that indicate the current state of the UserDefinedNetwork.

        Attributes:
            Type (class): Contains constants representing different status types.
            Reason (class): Contains constants representing various reasons for the status conditions.
        """

        class Type:
            """
            Defines the types of status conditions for the UserDefinedNetwork.

            Attributes:
                NETWORK_READY (str): Indicates that the network is ready.
            """

            NETWORK_READY: str = "NetworkReady"

        class Reason:
            """
            Defines the reasons for the status conditions of the UserDefinedNetwork.

            Attributes:
                NETWORK_ATTACHMENT_DEFINITION_READY (str): Indicates that the network attachment definition is ready.
                SYNC_ERROR (str): Indicates that there is a synchronization error.
            """

            NETWORK_ATTACHMENT_DEFINITION_READY: str = "NetworkAttachmentDefinitionReady"
            SYNC_ERROR: str = "SyncError"

    @classmethod
    def is_ready_condition(cls, condition: dict) -> bool:
        """
        Check if the given condition indicates that the UserDefinedNetwork is ready.

        Args:
            condition (dict): A dictionary representing the condition of the UserDefinedNetwork.

        Returns:
            bool: True if the condition indicates the UserDefinedNetwork is ready, False otherwise.
        """
        return (
            condition["reason"] == cls.Status.Reason.NETWORK_ATTACHMENT_DEFINITION_READY
            and condition["status"] == cls.Condition.Status.TRUE
            and condition["type"] == cls.Status.Type.NETWORK_READY
        )

    @classmethod
    def is_sync_error_condition(cls, condition: dict) -> bool:
        """
        Check if the given condition indicates a synchronization error for the UserDefinedNetwork.

        Args:
            condition (dict): A dictionary representing the condition of the UserDefinedNetwork.

        Returns:
            bool: True if the condition indicates a synchronization error, False otherwise.
        """
        return (
            condition["reason"] == cls.Status.Reason.SYNC_ERROR
            and condition["status"] == cls.Condition.Status.FALSE
            and condition["type"] == cls.Status.Type.NETWORK_READY
        )

    @property
    def conditions(self) -> list:
        """
        Retrieve the current status conditions of the UserDefinedNetwork instance.

        This property accesses the list of conditions from the status of the
        UserDefinedNetwork instance.

        Returns:
            list: A list of status conditions associated with the UserDefinedNetwork instance.
        """
        return self.instance.status.conditions

    @property
    def ready(self) -> bool:
        """
        Determine if the UserDefinedNetwork instance is ready.

        This property evaluates the current status conditions of the
        UserDefinedNetwork instance to check if any indicate that the instance
        is in a ready state.

        Returns:
            bool: True if the UserDefinedNetwork is ready; otherwise, False.
        """
        return any(self.is_ready_condition(condition=condition) for condition in self.conditions)

    @property
    def sync_error(self) -> bool:
        """
        Check for synchronization errors in the UserDefinedNetwork instance.

        This property assesses the current status conditions to identify if
        any indicate a synchronization error for the UserDefinedNetwork instance.

        Returns:
            bool: True if there is a synchronization error; otherwise, False.
        """
        return any(self.is_sync_error_condition(condition=condition) for condition in self.conditions)

    def wait_for_status_condition(
        self,
        wait_condition_fns: list,
        not_wait_condition_fns: Optional[list] = None,
        wait_timeout: int = 120,
        sleep_interval: int = 2,
    ) -> dict:
        """
        Wait for specific status conditions to be met.

        This function continuously checks the current status conditions, waiting for
        any of the specified "wait" conditions to be satisfied while monitoring for
        any of the specified "not wait" conditions to trigger a failure.

        Args:
            wait_condition_fns (list): A list of functions that determine if the
                desired status condition has been met.
            not_wait_condition_fns (Optional[list]): A list of functions that determine if
                a failure condition has occurred. Default is None
            wait_timeout (int): The maximum time to wait for the conditions to
                be satisfied (in seconds). Default is 120 seconds.
            sleep_interval (int): The interval between checks (in seconds).
                Default is 2 seconds.

        Returns:
            dict: The condition that indicates the desired status when met.

        Raises:
            WaitForStatusConditionFailed: If any of the unexpected conditions is met.
            TimeoutExpiredError: If the timeout expires before the conditions
                are satisfied.
        """
        samples = TimeoutSampler(wait_timeout=wait_timeout, sleep=sleep_interval, func=lambda: self.conditions)

        try:
            for sample in samples:
                for condition in sample:
                    if any(wait_fn(condition) for wait_fn in wait_condition_fns):
                        return condition

                if not_wait_condition_fns:
                    for condition in sample:
                        if any(not_wait_fn(condition) for not_wait_fn in not_wait_condition_fns):
                            raise WaitForStatusConditionFailed(
                                f"Failed to wait for the intended status for UDN {self.name}. "
                                f"Condition message: {condition['message']}"
                            )

        except (TimeoutExpiredError, WaitForStatusConditionFailed) as e:
            self.logger.error(f"{str(e)}")
            raise

        return {}

    def wait_for_status_condition_ready(
        self,
        wait_timeout: int = 120,
        sleep_interval: int = 2,
    ) -> dict:
        """
        Wait for the UserDefinedNetwork to reach a ready condition status.

        Args:
            wait_timeout (int, optional): The maximum time to wait for the condition
                to be met, in seconds. Default is 120 seconds.
            sleep_interval (int, optional): The time to sleep between status checks,
                in seconds. Default is 2 seconds.

        Returns:
            dict: The condition that indicates the desired status when met.

        Raises:
            WaitForStatusConditionFailed: If any of the unexpected conditions are met.
            TimeoutExpiredError: If the timeout expires before the conditions
                are satisfied.
        """
        return self.wait_for_status_condition(
            wait_condition_fns=[self.is_ready_condition],
            not_wait_condition_fns=[self.is_sync_error_condition],
            wait_timeout=wait_timeout,
            sleep_interval=sleep_interval,
        )


class TopologyType:
    """
    This class serves as a container for constants representing various types of network topologies
    used in the UserDefinedNetwork configuration.

    Attributes:
        LAYER2 (str): Represents a Layer2 topology.
        LAYER3 (str): Represents a Layer3 topology.
        LOCALNET (str): Represents a LocalNet topology.
    """

    LAYER2 = "Layer2"
    LAYER3 = "Layer3"
    LOCALNET = "LocalNet"


class Layer2UserDefinedNetwork(UserDefinedNetwork):
    """
    UserDefinedNetwork layer2 object.

    API reference:
    https://ovn-kubernetes.io/api-reference/userdefinednetwork-api-spec/#layer2config
    """

    def __init__(
        self,
        name: str,
        namespace: str,
        client: Optional[DynamicClient] = None,
        role: Optional[str] = None,
        mtu: Optional[int] = None,
        subnets: Optional[list] = None,
        join_subnets: Optional[str] = None,
        ipam_lifecycle: Optional[str] = None,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork with layer2 configuration

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): The namespace of the UserDefinedNetwork.
            client (Optional[DynamicClient]): DynamicClient to use.
            role (Optional[str]): role describes the network role in the pod.
            mtu (Optional[int]): mtu is the maximum transmission unit for a network.
            subnets (Optional[list]): subnets are used for the pod network across the cluster.
            join_subnets (Optional[str]): join_subnets are used inside the OVN network topology.
            ipam_lifecycle (Optional[str]): ipam_lifecycle controls IP addresses management lifecycle.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            topology=TopologyType.LAYER2,
            **kwargs,
        )
        self.role = role
        self.mtu = mtu
        self.subnets = subnets
        self.join_subnets = join_subnets
        self.ipam_lifecycle = ipam_lifecycle

    def to_dict(self) -> None:
        super().to_dict()
        if not self.yaml_file:
            self.res.setdefault("spec", {}).setdefault("layer2", {})

            attributes = {
                "role": self.role,
                "mtu": self.mtu,
                "subnets": self.subnets,
                "joinSubnets": self.join_subnets,
                "ipamLifecycle": self.ipam_lifecycle,
            }

            for key, value in attributes.items():
                if value is not None:
                    self.res["spec"]["layer2"][key] = value


class Layer3Subnets:
    """
    UserDefinedNetwork layer3 subnets object.

    API reference:
    https://ovn-kubernetes.io/api-reference/userdefinednetwork-api-spec/#layer3subnet
    """

    def __init__(
        self,
        cidr: Optional[str] = None,
        host_subnet: Optional[int] = None,
    ):
        """
        UserDefinedNetwork layer3 subnets object.

        Args:
            cidr (Optional[str]): CIDR specifies L3Subnet, which is split into smaller subnets for every node.
            host_subnet (Optional[int]): host_subnet specifies the subnet size for every node.
        """
        self.cidr = cidr
        self.host_subnet = host_subnet


class Layer3UserDefinedNetwork(UserDefinedNetwork):
    """
    UserDefinedNetwork layer3 object.

    API reference:
    https://ovn-kubernetes.io/api-reference/userdefinednetwork-api-spec/#layer3config
    """

    def __init__(
        self,
        name: str,
        namespace: str,
        client: Optional[DynamicClient] = None,
        role: Optional[str] = None,
        mtu: Optional[int] = None,
        subnets: Optional[list[Layer3Subnets]] = None,
        join_subnets: Optional[str] = None,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork with layer3 configuration

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): The namespace of the UserDefinedNetwork.
            client (Optional[DynamicClient]): DynamicClient to use.
            role (Optional[str]): role describes the network role in the pod.
            mtu (Optional[int]): mtu is the maximum transmission unit for a network.
            subnets (Optional[list[Layer3Subnets]]): subnets are used for the pod network across the cluster.
            join_subnets (Optional[str]): join_subnets are used inside the OVN network topology.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            topology=TopologyType.LAYER3,
            **kwargs,
        )
        self.role = role
        self.mtu = mtu
        self.subnets = subnets
        self.join_subnets = join_subnets

    def to_dict(self) -> None:
        super().to_dict()
        if not self.yaml_file:
            self.res.setdefault("spec", {}).setdefault("layer3", {})

            attributes = {
                "role": self.role,
                "mtu": self.mtu,
                "joinSubnets": self.join_subnets,
            }

            for key, value in attributes.items():
                if value is not None:
                    self.res["spec"]["layer3"][key] = value

            if self.subnets is not None:
                self.res["spec"]["layer3"].setdefault("subnets", [])

                for subnet in self.subnets:
                    subnet_dict = {
                        key: value
                        for key, value in [("cidr", subnet.cidr), ("hostSubnet", subnet.host_subnet)]
                        if value is not None
                    }
                    self.res["spec"]["layer3"]["subnets"].append(subnet_dict)


class LocalNetUserDefinedNetwork(UserDefinedNetwork):
    """
    UserDefinedNetwork localNet object.

    API reference:
    https://ovn-kubernetes.io/api-reference/userdefinednetwork-api-spec/#localnetconfig
    """

    def __init__(
        self,
        name: str,
        namespace: str,
        client: Optional[DynamicClient] = None,
        role: Optional[str] = None,
        mtu: Optional[int] = None,
        subnets: Optional[list] = None,
        exclude_subnets: Optional[list] = None,
        ipam_lifecycle: Optional[str] = None,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork with localNet configuration

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): The namespace of the UserDefinedNetwork.
            client (Optional[DynamicClient]): DynamicClient to use.
            role (Optional[str]): role describes the network role in the pod.
            mtu (Optional[int]): mtu is the maximum transmission unit for a network.
            subnets (Optional[list]): subnets are used for the pod network across the cluster.
            exclude_subnets (Optional[list]): exclude_subnets is a list of CIDRs that will be removed from the assignable
                IP address pool specified by the "Subnets" field.
            ipam_lifecycle (Optional[str]): ipam_lifecycle controls IP addresses management lifecycle.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            topology=TopologyType.LOCALNET,
            **kwargs,
        )
        self.role = role
        self.mtu = mtu
        self.subnets = subnets
        self.exclude_subnets = exclude_subnets
        self.ipam_lifecycle = ipam_lifecycle

    def to_dict(self) -> None:
        super().to_dict()
        if not self.yaml_file:
            self.res.setdefault("spec", {}).setdefault("localNet", {})

            attributes = {
                "role": self.role,
                "mtu": self.mtu,
                "subnets": self.subnets,
                "excludeSubnets": self.exclude_subnets,
                "ipamLifecycle": self.ipam_lifecycle,
            }

            for key, value in attributes.items():
                if value is not None:
                    self.res["spec"]["localNet"][key] = value
