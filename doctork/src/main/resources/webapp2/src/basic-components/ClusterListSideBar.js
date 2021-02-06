import React, { Component } from "react";
import ClusterCard from "./ClusterCard";
import { Box } from "gestalt";

class ClusterListSideBar extends Component {
  constructor(props) {
    super(props);
  }

  updateSelected(e, cluster, parent) {
    if ("state" in parent) {
      parent.setState({
        selectedCluster: cluster
      });
    }
  }

  render() {
    const parent = this.props.parent;
    const clusterList = this.props.clusters.map(cluster => (
      <li
        key={cluster.id}
        onClick={e => this.updateSelected(e, cluster, parent)}
      >
        <ClusterCard cluster={cluster}></ClusterCard>
      </li>
    ));
    return (
      <Box>
        <ul>{clusterList}</ul>
      </Box>
    );
  }
}

export default ClusterListSideBar;
