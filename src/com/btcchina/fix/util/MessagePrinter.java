package com.btcchina.fix.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import quickfix.DataDictionary;
import quickfix.Field;
import quickfix.FieldMap;
import quickfix.FieldNotFound;
import quickfix.FieldType;
import quickfix.Group;
import quickfix.field.MsgType;
import quickfix.fix44.Message;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;

public class MessagePrinter {

	private Connection conn = null;
	private PreparedStatement ps = null;
	private ResultSet set = null;
	private String driver = "com.mysql.jdbc.Driver";
	private String url = "jdbc:mysql://127.0.0.1:3306/btc";
	private String user = "root";
	private String password = "root";
	
	private static double avg = 0.0;
	private static double last = 0.0;
	private static int forcast = 0;
	
	private static int cnt = 0;
	private static int right = 0;	

	public MessagePrinter() {
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			ps = conn
					.prepareStatement("insert into trade(price, date) values(?, ?)");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void print(DataDictionary dd, Message message) throws FieldNotFound {
		String msgType = message.getHeader().getString(MsgType.FIELD);
		// printFieldMap("", dd, msgType, message.getHeader());
		printFieldMap("", dd, msgType, message);
		// printFieldMap("", dd, msgType, message.getTrailer());
		try {
			ps.executeBatch();
			ps.close();
			set = conn.prepareStatement("select avg(price) from trade where unix_timestamp() - 8*3600 - 1*60 < unix_timestamp(date)").executeQuery();
			if (set.next())
				avg = set.getDouble(1);
			set = conn.prepareStatement("select price from trade order by id desc limit 1").executeQuery();
			if (set.next())
				last = set.getDouble(1);
			if (last < avg) {
				forcast = 0; // down
			} else {
				forcast = 1; // up
			}
			
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	private void printFieldMap(String prefix, DataDictionary dd,
			String msgType, FieldMap fieldMap) throws FieldNotFound {

		Iterator<?> fieldIterator = fieldMap.iterator();
		boolean tag = false;
		// int type = 0;
		String str = "";
		while (fieldIterator.hasNext()) {
			Field<?> field = (Field<?>) fieldIterator.next();
			if (!isGroupCountField(dd, field)) {
				String value = fieldMap.getString(field.getTag());
				if (dd.hasFieldValue(field.getTag())) {
					// type = Integer.parseInt(value);
					value = dd.getValueName(field.getTag(),
							fieldMap.getString(field.getTag()))
							+ " (" + value + ")";
				}
				if (field.getTag() == 269 && value.equals("TRADE (2)") /*
																		 * &&
																		 * type
																		 * == 2
																		 */) {
					tag = true;
					// System.out.println(prefix +
					// dd.getFieldName(field.getTag()) + ": " + value + " # " +
					// field.getTag());
				}
				if (tag
						&& (field.getTag() == 270 || field.getTag() == 272 || field
								.getTag() == 273)) {
					// System.out.println(prefix +
					// dd.getFieldName(field.getTag()) + ": " + value + " # " +
					// field.getTag());
					str += value + " "; // System.out.println(value);

					// break;
				}
			}
		}
		if (tag) {
	    	System.out.println(str);
			String tmp[] = str.split(" ", 2);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");
			java.sql.Timestamp sqlDate = null;
		    try {
		    	Date date = sdf.parse(tmp[1]);
		    	sqlDate = new java.sql.Timestamp(date.getTime());
				ps.setDouble(1, Double.parseDouble(tmp[0]));
				ps.setTimestamp(2, sqlDate);
				ps.addBatch();
				if ((Double.parseDouble(tmp[0]) < last && forcast == 0)
						|| (Double.parseDouble(tmp[0]) >= last && forcast == 1)) {
					right++;
				}
				cnt++;
				System.out.println("STATUS: " + right + "\t" + cnt + "\t" + 100.0 * right / cnt + "%");
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		Iterator<?> groupsKeys = fieldMap.groupKeyIterator();
		while (groupsKeys.hasNext()) {
			int groupCountTag = ((Integer) groupsKeys.next()).intValue();
			// System.out.println(prefix + dd.getFieldName(groupCountTag) +
			// ": count = "
			// + fieldMap.getInt(groupCountTag));
			Group g = new Group(groupCountTag, 0);
			int i = 1;
			while (fieldMap.hasGroup(i, groupCountTag)) {
				if (i > 1) {
					// System.out.println(prefix + "  ----");
				}
				fieldMap.getGroup(i, g);
				printFieldMap(prefix + "  ", dd, msgType, g);
				i++;
			}
		}
	}

	private boolean isGroupCountField(DataDictionary dd, Field<?> field) {
		return dd.getFieldTypeEnum(field.getTag()) == FieldType.NumInGroup;
	}

	public static void main(String args[]) {
		MessagePrinter mp = new MessagePrinter();
		String symbol = mp.getFieldName("55");
		System.out.println("Symbol is " + symbol);
		String group = mp.getFieldName("269", "0");
		System.out.println("group is " + group);
	}

	public String getFieldName(String tag) {
		String field_name = "";
		try {
			File fixdic = new File("data/FIX44.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document parser = dBuilder.parse(fixdic);

			// optional, but recommended
			// read this -
			// http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			parser.getDocumentElement().normalize();
			// System.out.println("Root element :" +
			// doc.getDocumentElement().getNodeName());
			NodeList fieldList = parser.getElementsByTagName("field");
			// System.out.println("----------------------------");
			for (int fieldIndex = 0; fieldIndex < fieldList.getLength(); fieldIndex++) {
				Node fieldNode = fieldList.item(fieldIndex);
				// System.out.println("\nCurrent Element :" +
				// nNode.getNodeName());
				if (fieldNode.getNodeType() == Node.ELEMENT_NODE) {
					Element fieldElement = (Element) fieldNode;
					String field_number = fieldElement.getAttribute("number");
					// System.out.println("number : " + field_number);
					field_name = fieldElement.getAttribute("name");
					// System.out.println("Field Name : " + field_name);
					if (tag.equals(field_number)) {
						return field_name;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return field_name;
	}

	public String getFieldName(String group, String tag) {
		String field_name = "";
		try {
			File fixdic = new File("data/FIX44.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document parser = dBuilder.parse(fixdic);

			// optional, but recommended
			// read this -
			// http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			parser.getDocumentElement().normalize();
			NodeList fieldList = parser.getElementsByTagName("field");

			for (int fieldIndex = 0; fieldIndex < fieldList.getLength(); fieldIndex++) {
				Node fieldNode = fieldList.item(fieldIndex);
				if (fieldNode.getNodeType() == Node.ELEMENT_NODE) {
					Element fieldElement = (Element) fieldNode;
					String field_number = fieldElement.getAttribute("number");
					field_name = fieldElement.getAttribute("name");
					if (group.equals(field_number)) {
						NodeList value = fieldElement
								.getElementsByTagName("value");
						for (int leaf = 0; leaf < value.getLength(); leaf++) {
							Node valueNode = value.item(leaf);
							Element valueElement = (Element) valueNode;
							String num = valueElement.getAttribute("enum");
							field_name = valueElement
									.getAttribute("description");
							if (tag.equals(num)) {
								return field_name;
							}
						}
						return field_name;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return field_name;
	}

	public String getValue(String s) {
		if (s.indexOf("=") > 0) {
			s = s.substring(s.indexOf("=") + 1);
		}
		return s;
	}

	public String getNum(String s) {
		if (s.indexOf("=") > 0) {
			s = s.substring(s.indexOf("=") + 1);
		}
		return s;
	}

	public String getTag(String s) {
		if (s.indexOf("=") > 0) {
			s = s.substring(0, s.indexOf("="));
		}
		return s;
	}

}