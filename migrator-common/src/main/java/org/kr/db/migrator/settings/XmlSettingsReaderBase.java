package org.kr.db.migrator.settings;

import org.kr.db.migrator.exceptions.ReadSettingsException;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 10:57
 * To change this template use File | Settings | File Templates.
 */
public abstract class XmlSettingsReaderBase<T extends QueryBase> extends XmlReaderBase {

    private final Logger log = Logger.getLogger(XmlSettingsReaderBase.class);

    public XmlSettingsReaderBase(String file) throws ReadSettingsException {
        super(file);
    }

    protected ConnectionInfo readConnectionInfo() throws ReadSettingsException {
        ConnectionInfo.JmsConnectionInfo jmsConnectionInfo = getJmsConnectionInfo();
        ConnectionInfo.DatabaseConnectionInfo databaseConnectionInfo = getDatabaseConnectionInfo();
        return new ConnectionInfo(databaseConnectionInfo, jmsConnectionInfo);
    }

    protected ConnectionInfo.JmsConnectionInfo getJmsConnectionInfo() throws ReadSettingsException {
        Element xmlElement = (Element)evaluateXPath("/settings/connection/jms", XPathConstants.NODE);
        String brokerUrl = getAttributeValue(xmlElement, "broker_url");
        return new ConnectionInfo.JmsConnectionInfo(brokerUrl);
    }

    protected ConnectionInfo.DatabaseConnectionInfo getDatabaseConnectionInfo() throws ReadSettingsException {
        Element xmlElement = (Element)evaluateXPath("/settings/connection/database", XPathConstants.NODE);
        String driver = getAttributeValue(xmlElement, "driver");
        String url = getAttributeValue(xmlElement, "url");
        return new ConnectionInfo.DatabaseConnectionInfo(driver, url);
    }

    protected List<T> readActions() throws ReadSettingsException {
        List<T> queryList = new ArrayList<T>();
        queryList.addAll(readQueries());
        queryList.addAll(readTables());
        return queryList;
    }

    protected List<T> readQueries() throws ReadSettingsException {
        NodeList nodeList = (NodeList)evaluateXPath("/settings/actions/queries/query", XPathConstants.NODESET);
        List<T> queryList = new ArrayList<T>(nodeList.getLength());
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node queryNode = nodeList.item(i);
            T queryBase = parseQueryNode(queryNode);
            queryList.add(queryBase);
        }
        return queryList;
    }

    protected List<T> readTables() throws ReadSettingsException {
        NodeList nodeList = (NodeList)evaluateXPath("/settings/actions/tables/table", XPathConstants.NODESET);
        List<T> queryList = new ArrayList<T>(nodeList.getLength());
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node tableNode = nodeList.item(i);
            T queryBase = parseTableNode(tableNode);
            queryList.add(queryBase);
        }
        return queryList;
    }

    protected abstract T parseQueryNode(Node queryNode) throws ReadSettingsException;
    protected abstract T parseTableNode(Node tableNode) throws ReadSettingsException;

}
