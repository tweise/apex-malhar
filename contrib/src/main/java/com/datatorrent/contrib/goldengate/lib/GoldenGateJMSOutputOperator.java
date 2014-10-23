/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.lib;

import com.datatorrent.lib.io.jms.AbstractActiveMQSinglePortOutputOperator;
import com.goldengate.atg.datasource.DsOperation.OpType;
import java.text.SimpleDateFormat;
import javax.jms.JMSException;
import javax.jms.Message;

public class GoldenGateJMSOutputOperator extends AbstractActiveMQSinglePortOutputOperator<_DsTransaction>
{
  private transient SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

  @Override
  protected Message createMessage(_DsTransaction tuple)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("<t>");

    String date = dateFormat.format(tuple.getReadTime());

    for(_DsOperation op: tuple.getOps()) {
      if(op.getOperationType() != OpType.DO_INSERT) {
        continue;
      }

      sb.append("<o t='");
      sb.append(op.getTableName());
      sb.append("' s='I' d='");
      sb.append(date);
      sb.append("' p='");
      sb.append(op.getPositionSeqno());
      sb.append("'>");

      _DsColumn[] cols = op.getCols().toArray(new _DsColumn[] {});

      for(int columnCounter = 0;
          columnCounter < 3;
          columnCounter++) {
        sb.append("<c i='");
        sb.append(columnCounter);
        sb.append("'>");
        sb.append("<a><![CDATA[");
        sb.append(cols[columnCounter].getAfterValue());
        sb.append("]]></a></c>");
      }

      sb.append("</o>");
    }

    sb.append("</t>");
    Message message = null;

    try {
      message = getSession().createTextMessage(sb.toString());
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    return message;
  }
}
