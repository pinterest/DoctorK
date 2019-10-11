import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import React, { Component } from "react";

class Brokers extends Component {
  constructor(props) {
    super(props);
    this.state = {
      brokers: []
    };
  }

  fetchBrokers(clusterId) {
    fetch("/api/clusters/" + clusterId + "/brokers")
      .then(response => response.json())
      .then(data => this.setState({ brokers: data }));
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (this.props.clusterId !== nextProps.clusterId) {
      this.fetchBrokers(nextProps.clusterId);
    }
  }

  render() {
    let rows = this.state.brokers;
    return (
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Id</TableCell>
            <TableCell align="right">Hostname</TableCell>
            <TableCell align="right">Rack</TableCell>
            <TableCell align="right"># Topics</TableCell>
            <TableCell align="right"># Partitions</TableCell>
            <TableCell align="right"># Leaders</TableCell>
            <TableCell align="right">MB/s In</TableCell>
            <TableCell align="right">MB/s Out</TableCell>
            <TableCell align="right">Last Updated</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map(row => (
            <TableRow key={row.id}>
              <TableCell component="th" scope="row">
                {row.id}
              </TableCell>
              <TableCell align="right">{row.name}</TableCell>
              <TableCell align="right">{row.rackId}</TableCell>
              <TableCell align="right">{row.numTopics}</TableCell>
              <TableCell align="right">{row.numPartitions}</TableCell>
              <TableCell align="right">{row.numLeaders}</TableCell>
              <TableCell align="right">{Number(row.mbIn).toFixed(1)}</TableCell>
              <TableCell align="right">
                {Number(row.mbOut).toFixed(1)}
              </TableCell>
              <TableCell align="right">{row.lastStatsTimestamp}</TableCell>
              <TableCell align="right"></TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    );
  }
}

export default Brokers;
