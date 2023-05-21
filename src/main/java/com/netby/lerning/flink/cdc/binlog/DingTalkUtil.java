package com.netby.lerning.flink.cdc.binlog;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.taobao.api.internal.util.Base64;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

/**
 * 钉钉机器人报警消息
 *
 * @Author: byg
 * @Date: 2020/10/16 16:39
 */
@Slf4j
@Component
public class DingTalkUtil {

    private static String appName = "pa-mysql-binlog";

    private static String environment = "test";

    private final static String ALOG_HMACSHA256 = "HmacSHA256";
    private final static String ECODING_UTF8 = "UTF-8";
    private final static String SIGN_COMBINE = "\n";
    private final static String REQ_PARAM_TIMESTAMP = "&timestamp=";
    private final static String REQ_PARAM_SIGN = "&sign=";
    private final static String MSG_TYPE = "markdown";

    /**
     * 线下
     */
    private static String DEFAULT_NOTIFY_URL_OFFLINE = "https://oapi.dingtalk.com/robot/send?access_token=51069ad213197ba4c1ef5be83a3e2110a3fe29421eb1e6f748ab22cf5d34d272";
    private static String DEFAULT_NOTIFY_SECRET_OFFLINE = "SEC4e9d4f3b37363c28456f95bdb8c799ad5fa0a69f0fd1324c993b8c5c41e43c51";


    /**
     * 报警业务枚举，需指定上面的url和secret
     */
    public enum BizTypeEnum {
        /**
         * 线下默认
         */
        DEFAULT_OFFLINE(1, "线下默认报警", DEFAULT_NOTIFY_URL_OFFLINE, DEFAULT_NOTIFY_SECRET_OFFLINE),
        ;

        @Getter
        @Setter
        private Integer type;
        @Getter
        @Setter
        private String desc;
        @Getter
        @Setter
        private String notifyUrl;
        @Getter
        @Setter
        private String notifySecret;

        BizTypeEnum(Integer type, String desc, String notifyUrl, String notifySecret) {
            this.type = type;
            this.desc = desc;
            this.notifyUrl = notifyUrl;
            this.notifySecret = notifySecret;
        }
    }

    public static void main(String[] args) {
        sendTextMsg("", DEFAULT_NOTIFY_URL_OFFLINE, DEFAULT_NOTIFY_SECRET_OFFLINE, appName, "报警测试，请忽略", "Error");
    }

    /**
     * 发送error消息
     *
     * @param msg
     * @return
     */
    public static void sendErrorMsg(String msg) {
        sendErrorMsg(msg, null);
    }

    /**
     * 发送error消息
     *
     * @param msg
     * @return
     */
    public static void sendErrorMsg(String msg, BizTypeEnum typeEnum) {
        log.error("触发钉钉报警,msg={}", msg);
        String traceId = MDC.get("traceId");
        ExecutorUtil.nonCore().execute(() -> {
            sendTextMsg(traceId, getNotifyUrl(typeEnum), getNotifySecret(typeEnum), appName, msg, "Error");
        });
    }

    private static void sendTextMsg(String traceId, String url, String secret, String appName, String textMsg, String msgType) {
        StringBuilder msg = new StringBuilder();
        msg.append("#### 异常报警：").append(SIGN_COMBINE);
        msg.append("- 应用：").append(appName).append(SIGN_COMBINE);
        msg.append("- 反馈时间：").append(DateUtil.format(new Date(System.currentTimeMillis()), DatePattern.NORM_DATETIME_PATTERN)).append(SIGN_COMBINE);
        msg.append("- 反馈内容：").append(textMsg).append(SIGN_COMBINE);
        msg.append("- TraceId：").append(traceId).append(SIGN_COMBINE);
        OapiRobotSendResponse response = null;
        try {
            //获取基础数据
            String timestamp = String.valueOf(System.currentTimeMillis());
            StringBuilder signUrl = new StringBuilder(url);
            signUrl.append(REQ_PARAM_TIMESTAMP)
                    .append(timestamp)
                    .append(REQ_PARAM_SIGN)
                    .append(getRequestSign(secret, timestamp));
            //初始化client
            DingTalkClient client = new DefaultDingTalkClient(signUrl.toString());
            OapiRobotSendRequest request = new OapiRobotSendRequest();
            request.setMsgtype(MSG_TYPE);
            OapiRobotSendRequest.Markdown markdown = new OapiRobotSendRequest.Markdown();
            markdown.setTitle("异常报警");
            markdown.setText(msg.toString());
            request.setMarkdown(markdown);
            OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
            at.setIsAtAll(Boolean.TRUE);
            request.setAt(at);
            response = client.execute(request);
        } catch (Exception e) {
            log.error("发送钉钉消息异常,textMsg=" + textMsg, e);
        }
        if (response == null || !response.isSuccess()) {
            log.error("发送钉钉消息失败, textMsg={}, response= {}", textMsg, JSON.toJSONString(response));
        }
    }

    /**
     * 获取请求的签名
     *
     * @param secret
     * @param timestamp
     * @return
     * @throws UnsupportedEncodingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    private static String getRequestSign(String secret, String timestamp) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
        String stringToSign = timestamp + SIGN_COMBINE + secret;
        Mac mac = Mac.getInstance(ALOG_HMACSHA256);
        mac.init(new SecretKeySpec(secret.getBytes(ECODING_UTF8), ALOG_HMACSHA256));
        byte[] signData = mac.doFinal(stringToSign.getBytes(ECODING_UTF8));
        return URLEncoder.encode(Base64.encodeToString(signData, false), "UTF-8");
    }


    private static String getNotifyUrl(BizTypeEnum typeEnum) {
        try {
            // 线下环境
            if (isOfflineEnv() || isPressureTest() || typeEnum == null) {
                return DEFAULT_NOTIFY_URL_OFFLINE;
            }
            return typeEnum.getNotifyUrl();
        } catch (Exception e) {
            log.error("getNotifyUrl exception", e);
            return DEFAULT_NOTIFY_URL_OFFLINE;
        }
    }

    public static String getNotifySecret(BizTypeEnum typeEnum) {
        try {
            // 线下环境
            if (isOfflineEnv() || isPressureTest() || typeEnum == null) {
                return DEFAULT_NOTIFY_SECRET_OFFLINE;
            }

            return typeEnum.getNotifySecret();
        } catch (Exception e) {
            log.error("getNotifySecret exception", e);
            return DEFAULT_NOTIFY_SECRET_OFFLINE;
        }
    }

    /**
     * 是否是线下环境
     *
     * @return
     */
    public static boolean isOfflineEnv() {
        return "test".equals(environment) || "dev".equals(environment);
    }


    /**
     * 是否是压测
     *
     * @return
     */
    public static boolean isPressureTest() {
        return false;
    }

}
