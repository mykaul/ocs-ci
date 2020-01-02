import logging
import pytest

from ocs_ci.framework.testlib import (
    tier4, ignore_leftovers, ManageTest, aws_platform_required
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.resources import pod
from tests.sanity_helpers import Sanity
from tests.helpers import wait_for_ct_pod_recovery


logger = logging.getLogger(__name__)


@tier4
@ignore_leftovers
class TestNodesRestart(ManageTest):
    """
    Test ungraceful cluster shutdown
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """
        def finalizer():
            nodes.restart_nodes_teardown()
        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["force"],
        argvalues=[
            pytest.param(*[True], marks=pytest.mark.polarion_id("OCS-894")),
            pytest.param(
                *[False],
                marks=[pytest.mark.polarion_id("OCS-895"), aws_platform_required]
            )
        ]
    )
    def test_nodes_restart(self, nodes, pvc_factory, pod_factory, force):
        """
        Test nodes restart (from the platform layer, i.e, EC2 instances, VMWare VMs)

        """
        ocp_nodes = get_node_objs()
        nodes.restart_nodes(nodes=ocp_nodes, force=force)
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

    @pytest.mark.polarion_id("OCS-2015")
    def test_rolling_nodes_restart(self, nodes, pvc_factory, pod_factory):
        """
        Test restart nodes one after the other and check health status in between

        """
        ocp_nodes = get_node_objs()
        for node in ocp_nodes:
            nodes.restart_nodes(nodes=[node])
            self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

    @pytest.mark.parametrize(
        argnames=["resource", "operation"],
        argvalues=[
            pytest.param(*['rbd-provisioner', 'create_resources'], marks=[pytest.mark.polarion_id("OCS-1138")]),
            pytest.param(*['rbd-provisioner', 'delete_resources'], marks=[pytest.mark.polarion_id("OCS-1241")]),
            pytest.param(*['cephfs-provisioner', 'create_resources'], marks=[pytest.mark.polarion_id("OCS-1139")]),
            pytest.param(*['cephfs-provisioner', 'delete_resources'], marks=[pytest.mark.polarion_id("OCS-1242")]),
            pytest.param(*['rook-ceph-operator', 'create_resources'], marks=[pytest.mark.polarion_id("OCS-2016")]),
            pytest.param(*['rook-ceph-operator', 'delete_resources'], marks=[pytest.mark.polarion_id("OCS-2017")]),
        ]
    )
    def test_pv_provisioning_under_degraded_state(
        self, nodes, pvc_factory, pod_factory, resource, operation
    ):
        """
        Test PV provisioning under degraded state

        OCS-1138:
        - Stop 1 worker node that has the RBD provisioner
          pod running on
        - Wait for the RBD pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by creating resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        OCS-1241:
        - Stop 1 worker node that has the RBD provisioner
          pod running on
        - Wait for the RBD pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by deleting resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        OCS-1139:
        - Stop 1 worker node that has the CephFS provisioner
          pod running on
        - Wait for the CephFS pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by creating resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        OCS-1242:
        - Stop 1 worker node that has the CephFS provisioner
          pod running on
        - Wait for the CephFS pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by deleting resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        """
        if operation == 'delete_resources':
            # Create resources that their deletion will be tested later
            self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        resource_pods = None
        # Get the resource pod according to the resource type
        if resource == 'rook-ceph-operator':
            resource_pods = pod.get_operator_pods()[0]
        elif resource == 'rbd-provisioner':
            resource_pods = pod.get_rbdfsplugin_provisioner_pods()
        elif resource == 'cephfs-provisioner':
            resource_pods = pod.get_cephfsplugin_provisioner_pods()
        resource_pod = resource_pods[0]

        # Workaround for BZ 1778488 - https://github.com/red-hat-storage/ocs-ci/issues/1222
        if not resource == 'rook-ceph-operator':
            resource_node = pod.get_pod_node(resource_pod)
            rook_operator_pod = pod.get_operator_pods()[0]
            operator_node = pod.get_pod_node(rook_operator_pod)
            if operator_node.get().get('metadata').get(
                'name'
            ) == resource_node.get().get('metadata').get('name'):
                resource_pod = resource_pods[1]
        # End of workaround for BZ 1778488

        resource_pod_name = resource_pod.name
        logger.info(
            f"{resource} pod found: {resource_pod_name}"
        )

        # Get the node name that has the provisioner pod running on
        resource_node = pod.get_pod_node(resource_pod)
        resource_node_name = resource_node.get().get('metadata').get('name')
        logger.info(
            f"{resource} pod is running on node {resource_node_name}"
        )

        # Stopping the nodes
        nodes.stop_nodes(nodes=[resource_node])

        # Wait for the provisioner pod to get to running status
        selector = constants.CSI_RBDPLUGIN_PROVISIONER_LABEL if (
            resource == 'rbd'
        ) else constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL

        # Wait for the provisioner pod to reach Terminating status
        logger.info(
            f"Waiting for pod {resource_node_name} to reach status Terminating"
        )
        assert resource_pod.ocp.wait_for_resource(
            timeout=600, resource_name=resource_pod.name,
            condition=constants.STATUS_TERMINATING
        ), f"{resource} pod failed to reach status Terminating"
        logger.info(
            f"Pod {resource_node_name} has reached status Terminating"
        )

        # Wait for the provisioner pod to be started and reach running status
        logger.info(
            f"Waiting for pod {resource_node_name} to reach status Running"
        )
        logger.info(
            f"Pod {resource_node_name} has reached status Running"
        )

        # After this change https://github.com/rook/rook/pull/3642/, there are
        # 2 provisioners for each interface
        assert resource_pod.ocp.wait_for_resource(
            timeout=600, condition=constants.STATUS_RUNNING, selector=selector,
            resource_count=2
        ), f"{resource} pod failed to reach status Running"

        assert wait_for_ct_pod_recovery(), "Ceph tools pod failed to come up on another node"

        if operation == 'create_resources':
            # Cluster validation (resources creation and IO running)
            self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        elif operation == 'delete_resources':
            # Cluster validation (resources creation and IO running)
            self.sanity_helpers.delete_resources()

        # Starting the nodes
        nodes.start_nodes(nodes=[resource_node])

        # Checking cluster and Ceph health
        self.sanity_helpers.health_check()
