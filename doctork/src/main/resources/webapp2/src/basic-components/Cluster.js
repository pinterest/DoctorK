import { Box, Container, Tabs, Text } from "gestalt";
import React, { Component, Router, Route } from "react";
import Topics from "./Topics";
import Brokers from "./Brokers";

class Cluster extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeIndex: 0,
      tabActivationSequence: ["flex", "visuallyHidden", "visuallyHidden"]
    };
    this.handleChange = this._handleChange.bind(this);
  }

  _handleChange({ activeTabIndex, event }) {
    event.preventDefault();
    this.state.tabActivationSequence[this.state.activeIndex] = "visuallyHidden";
    this.setState({
      activeIndex: activeTabIndex
    });
    this.state.tabActivationSequence[activeTabIndex] = "flex";
    const tabName = this.state.tabs[activeTabIndex].href;
  }

  render() {
    if ("id" in this.props.clusterSummary) {
      this.state.tabs = [
        {
          text: "Topics(" + this.props.clusterSummary.numTopics + ")",
          href: "#topics"
        },
        {
          text: "Brokers(" + this.props.clusterSummary.numBrokers + ")",
          href: "#brokers"
        },
        {
          text: "Operations(" + this.props.clusterSummary.operations + ")",
          href: "#operations"
        }
      ];
      return (
        <Box display="flex" direction="row" padding={1}>
          <Container>
            {/* Cluster summary */}
            <Box direction="row" paddingY={1}>
              <Text bold size="xl">
                Cluster: {this.props.clusterSummary.name}
              </Text>
            </Box>
            <Box display="flex" direction="row">
              <Text>Zookeeper: {this.props.clusterSummary.zk}</Text>
            </Box>
            <Box paddingY={2}>
              <Tabs
                tabs={this.state.tabs}
                activeTabIndex={this.state.activeIndex}
                onChange={this.handleChange}
              />
            </Box>
            <Box display={this.state.tabActivationSequence[0]}>
              <Topics clusterId={this.props.clusterSummary.name}></Topics>
            </Box>
            <Box display={this.state.tabActivationSequence[1]}>
              <Brokers clusterId={this.props.clusterSummary.name}></Brokers>
            </Box>
          </Container>
        </Box>
      );
    } else {
      return <Box></Box>;
    }
  }
}

export default Cluster;
