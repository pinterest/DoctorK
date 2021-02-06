import "gestalt/dist/gestalt.css";
import React from "react";
import "./App.css";
import Layout from "./basic-components/Layout";

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }
  render() {
    return (
      <div className="App">
        <Layout></Layout>
      </div>
    );
  }
}

export default App;
