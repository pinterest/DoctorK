import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import React, { Component } from "react";

class Topics extends Component {
  constructor(props) {
    super(props);
    this.state = {
      topics: []
    };
  }

  fetchTopics(clusterId) {
    if (clusterId !== null) {
      fetch("/api/clusters/" + clusterId + "/topics")
        .then(response => response.json())
        .then(data => this.setState({ topics: data }));
    }
  }

  componentDidMount() {
    this.fetchTopics(this.props.clusterId);
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (this.props.clusterId !== nextProps.clusterId) {
      this.fetchTopics(nextProps.clusterId);
    }
  }

  render() {
    let rows = this.state.topics;
    return (
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Topic Name</TableCell>
            <TableCell align="right"># Partitions</TableCell>
            <TableCell align="right">Replication Factor</TableCell>
            <TableCell align="right">MB/s In</TableCell>
            <TableCell align="right">MB/s Out</TableCell>
            <TableCell align="right">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map(row => (
            <TableRow key={row.name}>
              <TableCell component="th" scope="row" sortDirection={false}>
                {row.name}
              </TableCell>
              <TableCell align="right">{row.numPartitions}</TableCell>
              <TableCell align="right">{row.replicationFactor}</TableCell>
              <TableCell align="right">
                {Number(row.mbsIn).toFixed(1)}
              </TableCell>
              <TableCell align="right">
                {Number(row.mbsOut).toFixed(1)}
              </TableCell>
              <TableCell align="right"></TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    );
  }
}

export default Topics;
