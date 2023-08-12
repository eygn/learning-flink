package com.netby.flink.cdc.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author baiyg
 * @date 2023/8/7 16:55
 */
@Slf4j
public class BinLogLifecycleListener implements BinaryLogClient.LifecycleListener {

    @Override
    public void onConnect(BinaryLogClient client) {
        log.info("binlog onConnect,position:{}", client.getBinlogFilename() + "/" + client.getBinlogPosition());
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
        log.error("onCommunicationFailure", ex);
        sendNotify(client, "onCommunicationFailure", ex);
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
        if (ex instanceof EventDataDeserializationException) {
            return;
        }
        log.error("onEventDeserializationFailure", ex);
        sendNotify(client, "onEventDeserializationFailure", ex);
    }

    @Override
    public void onDisconnect(BinaryLogClient client) {
        log.info("binlog onDisconnect,position:{}", client.getBinlogFilename() + "/" + client.getBinlogPosition());
        sendNotify(client, "onDisconnect", null);
    }

    private void sendNotify(BinaryLogClient client, String msg, Exception ex) {
        String errorMsg = "binlog " + msg + " exception,tableName:" + BinlogConfigContext.mysqlConfig.getDb() + "." +
                BinlogConfigContext.mysqlConfig.getTable() + ",binlogPosition:" + client.getBinlogFilename() + "/" + client.getBinlogPosition();
        if (ex != null) {
            errorMsg += ",exception:" + ex.getMessage();
        }
        DingTalkUtil.sendErrorMsg(errorMsg);
    }
}
