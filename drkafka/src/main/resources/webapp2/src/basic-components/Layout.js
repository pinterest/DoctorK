import { Box, Column, Icon, Text } from "gestalt";
import React, { Component } from "react";
import ClusterListSideBar from "./ClusterListSideBar";
import Cluster from "./Cluster";

class Layout extends Component {
  constructor(props) {
    super(props);
    this.state = {
      clusters: [],
      selectedCluster: {},
      showError: false
    };
  }

  componentDidMount() {
    console.log("Retrying");
    fetch("/api/clusters")
      .then(response => {
        if (!response.ok) {
          this.setState({ showError: true });
          // trigger a delayed retry
          setTimeout(this.componentDidMount.bind(this), 5000);
          return [];
        }
        return response.json();
      })
      .then(data => {
        this.setState({ clusters: data, selectedCluster: data[0] });
        return data;
      });
  }

  render() {
    if (this.state.showError) {
      return (
        <Box>
          <Box display="flex" direction="row" paddingY={1} color="midnight">
            <Icon
              icon="pinterest"
              accessibilityLabel="Pinterest"
              size={30}
              color="red"
            />
            &nbsp;
            <Text bold inline color="white" align="center" size="xl">
              DoctorKafka
            </Text>
          </Box>
          <Box marginTop={10}>
            <Text align="center">
              No Clusters currently available to view. If Dr. Kafka was recently
              started please wait a few minutes before checking.
            </Text>
            <Text align="center">UI will attempt auto-refresh in 5s.</Text>
          </Box>
        </Box>
      );
    } else {
      return (
        <Box>
          <Box display="flex" direction="row" paddingY={1} color="midnight">
            <Icon
              icon="pinterest"
              accessibilityLabel="Pinterest"
              size={30}
              color="red"
            />
            &nbsp;
            <Text bold inline color="white" align="center" size="xl">
              DoctorKafka
            </Text>
          </Box>
          <Box display="flex" direction="row">
            <Column span={2}>
              <ClusterListSideBar
                parent={this}
                clusters={this.state.clusters}
              ></ClusterListSideBar>
            </Column>
            <Column span={10}>
              <Box direction="row" paddingX={2} paddingY={2}>
                <Cluster clusterSummary={this.state.selectedCluster}></Cluster>
              </Box>
            </Column>
          </Box>
        </Box>
      );
    }
  }
}

export default Layout;
