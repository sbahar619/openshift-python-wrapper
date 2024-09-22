from typing import Optional, Dict, Any

from timeout_sampler import TimeoutSampler, TimeoutExpiredError

from ocp_resources.resource import NamespacedResource

class StatusConditionFailed(Exception):
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
        name=None,
        namespace=None,
        client=None,
        topology: str = None,
        layer2: Optional[Dict[str, Any]] = None,
        layer3: Optional[Dict[str, Any]] = None,
        local_net: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): Namespace of the UserDefinedNetwork.
            client (DynamicClient): DynamicClient to use.
            topology (str): Topology describes network configuration.
            layer2 (Dict[str, Any]): Layer2 is the Layer2 topology configuration.
            local_net (Dict[str, Any]): LocalNet is the LocalNet topology configuration.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            *args,
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
            if self.topology:
                self.res["spec"]["topology"] = self.topology
            if self.layer2:
                self.res["spec"]["layer2"] = self.layer2
            if self.layer3:
                self.res["spec"]["layer3"] = self.layer3
            if self.local_net:
                self.res["spec"]["local_net"] = self.local_net

    class Status(NamespacedResource.Condition):
        class Type:
            NETWORK_READY: str = "NetworkReady"

        class Reason:
            NETWORK_ATTACHMENT_DEFINITION_READY = "NetworkAttachmentDefinitionReady"
            SYNC_ERROR = "SyncError"

    def is_ready_condition(self, condition):
        return (
                condition["reason"] == self.Status.Reason.NETWORK_ATTACHMENT_DEFINITION_READY and
                condition["status"] == self.Condition.Status.TRUE and
                condition["type"] == self.Status.Type.NETWORK_READY
        )

    def is_sync_error_condition(self, condition):
        return (
            condition["reason"] == self.Status.Reason.SYNC_ERROR and
            condition["status"] == self.Condition.Status.FALSE and
            condition["type"] == self.Status.Type.NETWORK_READY
        )

    @property
    def conditions(self):
        return self.instance.status.conditions

    @property
    def ready(self):
        return any(
            self.is_ready_condition(condition=condition)
            for condition in self.conditions
        )

    @property
    def sync_error(self):
        return any(
            self.is_sync_error_condition(condition=condition)
            for condition in self.conditions
        )

    def wait_for_status_condition(
            self,
            wait_condition_fns,
            not_wait_condition_fns,
            wait_timeout=120,
            sleep_interval=2
    ):
        """
        Wait for specific status conditions to be met.

        This function continuously checks the current status conditions, waiting for
        any of the specified "wait" conditions to be satisfied while monitoring for
        any of the specified "not wait" conditions to trigger a failure.

        Args:
            wait_condition_fns (list): A list of functions that determine if the
                desired status condition has been met.
            not_wait_condition_fns (list): A list of functions that determine if
                a failure condition has occurred.
            wait_timeout (int): The maximum time to wait for the conditions to
                be satisfied (in seconds). Default is 120 seconds.
            sleep_interval (int): The interval between checks (in seconds).
                Default is 2 seconds.

        Returns:
            dict: The condition that indicates the desired status when met.

        Raises:
            StatusConditionFailed: If a "not wait" condition is met.
            TimeoutExpiredError: If the timeout expires before the conditions
                are satisfied.
        """
        samples = TimeoutSampler(wait_timeout=wait_timeout, sleep=sleep_interval, func=lambda: self.conditions)

        try:
            for sample in samples:
                for condition in sample:
                    if any(wait_fn(condition) for wait_fn in wait_condition_fns):
                        return condition

                for condition in sample:
                    if any(not_wait_fn(condition) for not_wait_fn in not_wait_condition_fns):
                        raise StatusConditionFailed(
                            f"Failed to wait for the intended status for UDN {self.name}. "
                            f"Condition message: {condition['message']}"
                        )

        except (TimeoutExpiredError, StatusConditionFailed) as e:
            self.logger.error(f"{str(e)}")
            raise

    def wait_for_status_condition_ready(self):
        self.wait_for_status_condition(
            wait_condition_fns=[self.is_ready_condition],
            not_wait_condition_fns=[self.is_sync_error_condition]
        )

class TopologyType():
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
        name=None,
        namespace=None,
        client=None,
        role: str = None,
        mtu: int = None,
        subnets: list = None,
        join_subnets: str = None,
        ipam_lifecycle: str = None,
        *args,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork with layer2 configuration

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): Namespace of the UserDefinedNetwork.
            client (DynamicClient): DynamicClient to use.
            role (str): role describes the network role in the pod.
            mtu (int): mtu is the maximum transmission unit for a network.
            subnets (list): subnets are used for the pod network across the cluster.
            join_subnets (str): join_subnets are used inside the OVN network topology.
            ipam_lifecycle (str): ipam_lifecycle controls IP addresses management lifecycle.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            topology=TopologyType.LAYER2,
            *args,
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
                "ipamLifecycle": self.ipam_lifecycle
            }

            for key, value in attributes.items():
                if value:
                    self.res["spec"]["layer2"][key] = value


class Layer3Subnets:
    """
    UserDefinedNetwork layer3 subnets object.

    API reference:
    https://ovn-kubernetes.io/api-reference/userdefinednetwork-api-spec/#layer3subnet
    """
    def __init__(
            self,
            cidr: str = None,
            host_subnet: int = None,
    ):
        """
        UserDefinedNetwork layer3 subnets object.

        Args:
            cidr (str): CIDR specifies L3Subnet, which is split into smaller subnets for every node.
            host_subnet (int): host_subnet specifies the subnet size for every node.
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
        name=None,
        namespace=None,
        client=None,
        role: str = None,
        mtu: int = None,
        subnets: list[Layer3Subnets] = None,
        join_subnets: str = None,
        *args,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork with layer3 configuration

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): Namespace of the UserDefinedNetwork.
            client (DynamicClient): DynamicClient to use.
            role (str): role describes the network role in the pod.
            mtu (int): mtu is the maximum transmission unit for a network.
            subnets (list[Layer3Subnets]): subnets are used for the pod network across the cluster.
            join_subnets (str): join_subnets are used inside the OVN network topology.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            topology=TopologyType.LAYER3,
            *args,
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
                if value:
                    self.res["spec"]["layer3"][key] = value

            if self.subnets:
                self.res["spec"]["layer3"].setdefault("subnets", [])

                for subnet in self.subnets:
                    subnet_dict = {
                        "cidr": subnet.cidr,
                        "hostSubnet": subnet.host_subnet,
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
        name=None,
        namespace=None,
        client=None,
        role: str = None,
        mtu: int = None,
        subnets: list = None,
        exclude_subnets: list = None,
        ipam_lifecycle: str = None,
        *args,
        **kwargs,
    ):
        """
        Create and manage UserDefinedNetwork with localNet configuration

        Args:
            name (str): The name of the UserDefinedNetwork.
            namespace (str): Namespace of the UserDefinedNetwork.
            client (DynamicClient): DynamicClient to use.
            role (str): role describes the network role in the pod.
            mtu (int): mtu is the maximum transmission unit for a network.
            subnets (list): subnets are used for the pod network across the cluster.
            exclude_subnets (list): exclude_subnets is a list of CIDRs that will be removed from the assignable
                IP address pool specified by the "Subnets" field.
            ipam_lifecycle (str): ipam_lifecycle controls IP addresses management lifecycle.
        """
        super().__init__(
            name=name,
            namespace=namespace,
            client=client,
            topology=TopologyType.LOCALNET,
            *args,
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
                "ipamLifecycle": self.ipam_lifecycle
            }

            for key, value in attributes.items():
                if value:
                    self.res["spec"]["localNet"][key] = value