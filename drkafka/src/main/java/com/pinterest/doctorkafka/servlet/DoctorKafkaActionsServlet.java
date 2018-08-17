package com.pinterest.doctorkafka.servlet;

import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.OperatorAction;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.util.OperatorUtil;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class DoctorKafkaActionsServlet extends HttpServlet {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaActionsServlet.class);
  private static final String OPERATOR_ACTIONS_CONSUMER_GROUP = "doctorkafka_actions_consumer";
  private static final int NUM_MESSAGES = 1000;
  private static final long CONSUMER_POLL_TIMEOUT_MS = 1000L;
  private static final DecoderFactory avroDecoderFactory = DecoderFactory.get();
  private static Schema operatorActionSchema = OperatorAction.getClassSchema();

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    LOG.info("OperatorActionServlet starts halding get request");
    resp.setStatus(HttpStatus.OK_200);

    PrintWriter writer = resp.getWriter();
    renderHTML(writer);
  }

  private void renderHTML(PrintWriter writer) {
    DoctorKafkaServletUtil.printHeader(writer);
    writer.print("<div> <p><a href=\"/\">Home</a> > doctorkafka action </p> </div>");
    writer.print("<table class=\"table table-hover\"> ");
    writer.print("<th class=\"active\"> Timestamp </th> ");
    writer.print("<th class=\"active\"> Cluster </th> ");
    writer.print("<th class=\"active\"> Action </th>");

    try {
      retrieveActionReportMessages(writer);
    } catch (Exception e) {
      LOG.error("Failed to get actions", e);
      e.printStackTrace(writer);
    }
    writer.print("</tbody> </table>");
    DoctorKafkaServletUtil.printFooter(writer);
  }


  private void retrieveActionReportMessages(PrintWriter writer) {
    DoctorKafkaConfig doctorKafkaConfig = DoctorKafkaMain.doctorKafka.getDoctorKafkaConfig();
    String zkUrl = doctorKafkaConfig.getBrokerstatsZkurl();
    String actionReportTopic = doctorKafkaConfig.getActionReportTopic();
    Properties properties =
        OperatorUtil.createKafkaConsumerProperties(zkUrl, OPERATOR_ACTIONS_CONSUMER_GROUP,
            doctorKafkaConfig.getActionReportProducerSecurityProtocol(),
            doctorKafkaConfig.getActionReportProducerSslConfigs());
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);

    TopicPartition operatorReportTopicPartition = new TopicPartition(actionReportTopic, 0);
    List<TopicPartition> tps = new ArrayList<>();
    tps.add(operatorReportTopicPartition);
    consumer.assign(tps);

    Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(tps);
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
    for (TopicPartition tp : endOffsets.keySet()) {
      long numMessages = endOffsets.get(tp) - beginOffsets.get(tp);
      LOG.info("{} : offsets [{}, {}], num messages : {}",
          tp, beginOffsets.get(tp), endOffsets.get(tp), numMessages);
      consumer.seek(tp, Math.max(beginOffsets.get(tp), endOffsets.get(tp) - NUM_MESSAGES));
    }

    ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_TIMEOUT_MS);
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    while (!records.isEmpty()) {
      for (ConsumerRecord<byte[], byte[]> record : records) {
        recordList.add(record);
      }
      records = consumer.poll(CONSUMER_POLL_TIMEOUT_MS);
    }

    LOG.info("Read {} messages", recordList.size());
    for (ConsumerRecord<byte[], byte[]> record : Lists.reverse(recordList)) {
      try {
        BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(record.value(), null);
        SpecificDatumReader<OperatorAction> reader =
            new SpecificDatumReader<>(operatorActionSchema);

        OperatorAction result = new OperatorAction();
        reader.read(result, binaryDecoder);

        Date date = new Date(result.getTimestamp());
        writer.println("<tr class=\"active\"> ");
        writer.println("<td>" + dtFormat.format(date) + "</td>");
        writer.println("<td>" + result.getClusterName() + "</td>");
        writer.println("<td> " + result.getDescription() + "</td>");
        writer.println("</tr>");
      } catch (Exception e) {
        LOG.info("Fail to decode an message", e);
      }
    }
  }
}