from typing import Optional, Dict, Any

from ocp_resources.resource import NamespacedResource


class TopologyType():
    LAYER2 = "Layer2"
    LAYER3 = "Layer3"
    LOCALNET = "LocalNet"


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

    @property
    def ready(self):
        return any(
            stat["reason"] == self.Status.Reason.NETWORK_ATTACHMENT_DEFINITION_READY and
            stat["status"]  == self.Condition.Status.TRUE and
            stat["type"] == self.Status.Type.NETWORK_READY
            for stat in self.instance.status.conditions
        )

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
            subnets (list) subnets are used for the pod network across the cluster.
            join_subnets (str) join_subnets are used inside the OVN network topology.
            ipam_lifecycle (str) ipam_lifecycle controls IP addresses management lifecycle.
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
        subnets: list = None,
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
            subnets (list) subnets are used for the pod network across the cluster.
            join_subnets (str) join_subnets are used inside the OVN network topology.
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
                "subnets": self.subnets,
                "joinSubnets": self.join_subnets,
            }

            for key, value in attributes.items():
                if value:
                    self.res["spec"]["layer3"][key] = value