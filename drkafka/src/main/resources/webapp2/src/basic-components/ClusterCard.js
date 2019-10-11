import { Box, Column, Icon, Text } from "gestalt";
import React, { Component } from "react";

class ClusterCard extends Component {
  render() {
    const cluster = this.props.cluster;
    return (
      <Box direction="row" display="flex" color="darkWash" overflow="hidden">
        <Column span={7}>
          <Box display="flex" paddingX={2} paddingY={4} overflow="hidden">
            <Text bold size="md" accessibilityLabel={cluster.name}>
              {cluster.name}
            </Text>
          </Box>
        </Column>
        <Column>
          <Box direction="row" display="flex" paddingY={4}>
            <Box direction="row" display="flex" paddingX={1}>
              <Icon
                icon="alert"
                accessibilityLabel="Alert"
                color="gray"
                size={16}
              />
              <Text>{cluster.alerts}</Text>
            </Box>
            <Box direction="row" display="flex">
              <Icon
                icon="cog"
                accessibilityLabel="Operations"
                color="gray"
                size={16}
              />
              <Text>{cluster.operations}</Text>
            </Box>
          </Box>
        </Column>
      </Box>
    );
  }
}

export default ClusterCard;
