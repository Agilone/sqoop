/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.lib.FieldFormatter;

import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.inadco.ecoadapters.EcoUtil;

import java.lang.reflect.Method;

/**
 * Exports records from an Avro data file.
 */
public class ProtobufExportMapper
extends AutoProgressMapper<Text, BytesWritable,
SqoopRecord, NullWritable> {

	public static final Log LOG = LogFactory.getLog(
			ProtobufExportMapper.class.getName());

	private MapWritable columnTypes;
	private SqoopRecord recordImpl;
	private transient Descriptors.Descriptor m_msgDesc;
	private transient DelimiterSet delSet = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
	private Map<String,Integer> intMap = new HashMap<String,Integer>();

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		super.setup(context);
		com.cloudera.sqoop.lib.RecordParser s;
		Configuration conf = context.getConfiguration();

		String recordClassName = conf.get(
				ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY);
		if (null == recordClassName) {
			throw new IOException("Export table class name ("
					+ ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY
					+ ") is not set!");
		}

		try {
			Class cls = Class.forName(recordClassName, true,
					Thread.currentThread().getContextClassLoader());
			recordImpl = (SqoopRecord) ReflectionUtils.newInstance(cls, conf);
		} catch (ClassNotFoundException cnfe) {
			throw new IOException(cnfe);
		}

		if (null == recordImpl) {
			throw new IOException("Could not instantiate object of type "
					+ recordClassName);
		}

		int i=0;
		for(Entry<String, Object> s1 : recordImpl.getFieldMap().entrySet()) {
			LOG.info(s1.getKey()+":"+s1.getValue());
			intMap.put(s1.getKey(), i);
			i++;
		}

		LOG.info("Proto scheme "+conf.get("protobuf-scheme"));

		String className = conf.get("protobuf-scheme");

		try {
            if (className.startsWith("hdfs://"))
                m_msgDesc = EcoUtil.inferDescriptorFromFilesystem(className);
            else
                m_msgDesc = EcoUtil.inferDescriptorFromClassName(className);
        } catch (IOException e) {
            throw e;
        } catch (Throwable thr) {
            throw new IOException(thr);
        }
	}

	@Override
	protected void map(Text key, BytesWritable bw,
			Context context) throws IOException, InterruptedException {

		DynamicMessage msg = DynamicMessage.parseFrom(m_msgDesc,
				CodedInputStream.newInstance(bw.getBytes(), 0, bw.getLength()));

		String[] arr = new String[intMap.size()];

		for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : msg.getAllFields().entrySet()) {
			Descriptors.FieldDescriptor fd = entry.getKey();
			Object val = entry.getValue();

			if(intMap.get(fd.getName()) != null) {
				arr[intMap.get(fd.getName())] = FieldFormatter.escapeAndEnclose(""+val, delSet); // FieldFormatter.hiveStringDropDelims(""+val, delSet);

			}
		}

		String output = "";

		for(String s : arr) {
			if(output.length() != 0) {
				output += ",";
			}
			output += s;
		}

		try {
			recordImpl.parse(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
		context.write(recordImpl, NullWritable.get());
	}

}
