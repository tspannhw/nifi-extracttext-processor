/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataflowdeveloper.processors.process;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.xml.sax.SAXException;

@Tags({ "extracttextprocessortika" })
@CapabilityDescription("Run Apache Tika Text Extraction from PDF, Word, Excel.   Parameter for HTML or TEXT output.  Parameter for Maximum Length returned.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ExtractTextProcessor extends AbstractProcessor {

	private static final String TEXT_PLAIN = "text/plain";
	public static final String TEXT_HTML = "text/html";
	public static final String HTML_FORMAT = "html";
	public static final String TEXT_FORMAT = "text";

	public static final String FIELD_HTML_OUTPUT = "HTML_OUTPUT";
	public static final String FIELD_MAX_TEXT_LENGTH = "MAX_TEXT_LENGTH";
	public static final String FIELD_SUCCESS = "success";
	public static final String FIELD_FAILURE = "failure";

	public static final PropertyDescriptor MAX_TEXT_LENGTH = new PropertyDescriptor.Builder()
			.name(FIELD_MAX_TEXT_LENGTH).displayName("Max Output Text Length")
			.description(
					"The maximum length of text to retrieve. This is used to limit memory usage for dealing with large files. Specify -1 for unlimited length.")
			.required(false).defaultValue("-1").addValidator(StandardValidators.INTEGER_VALIDATOR)
			.expressionLanguageSupported(false).build();

	public static final PropertyDescriptor HTML_OUTPUT = new PropertyDescriptor.Builder()
			.name(FIELD_HTML_OUTPUT)
			.displayName("HTML Output instead of text")
			.description("Send html for HTML output or text for Text output")
			.required(false).defaultValue(TEXT_FORMAT)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.expressionLanguageSupported(false).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name(FIELD_SUCCESS)
			.description("Successfully extract content.").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name(FIELD_FAILURE)
			.description("Failed to extract content.").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(MAX_TEXT_LENGTH);
		descriptors.add(HTML_OUTPUT);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		return;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			flowFile = session.create();
		}

		final int maxTextLength = context.getProperty(MAX_TEXT_LENGTH).asInteger();
		final String outputMode = context.getProperty(HTML_OUTPUT).getValue();
		final String filename = flowFile.getAttribute("filename");
		
		try {
			final AtomicReference<String> type = new AtomicReference<>();
			final AtomicReference<Boolean> wasError = new AtomicReference<>(false);

			flowFile = session.write(flowFile, new StreamCallback() {
				@Override
				public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
					BufferedInputStream buffStream = new BufferedInputStream(inputStream);
					Tika tika = new Tika();
					String text = "";
					try {
						type.set(tika.detect(buffStream, filename));
						
						if (outputMode.equals(HTML_FORMAT)) {
							// http://lifeinide.com/post/2013-10-18-convert-document-to-html-with-apache-tika/
						
							ByteArrayOutputStream out = new ByteArrayOutputStream();
							SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
							TransformerHandler handler = factory.newTransformerHandler();
							handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "html");
							handler.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
							handler.getTransformer().setOutputProperty(OutputKeys.ENCODING, "UTF-8");
							handler.setResult(new StreamResult(out));
							ExpandedTitleContentHandler handler1 = new ExpandedTitleContentHandler(handler);

							AutoDetectParser parser = new AutoDetectParser();
							parser.parse(buffStream, handler1, new Metadata());
							text = new String(out.toByteArray(), "UTF-8");
						} else {
							tika.setMaxStringLength(maxTextLength);
							text = tika.parseToString(buffStream);
						}

					} catch (TikaException e) {
						getLogger().error("Apache Tika failed to parse input " + e.getLocalizedMessage());
						wasError.set(true);
						return;
					} catch (SAXException e) {
						getLogger().error(
								"Apache Tika failed to parse input on XML/HTML error " + e.getLocalizedMessage());
						wasError.set(true);
					} catch (TransformerConfigurationException e) {
						getLogger().error(
								"Apache Tika failed to parse input on XML/HTML error " + e.getLocalizedMessage());
						wasError.set(true);
					}

					outputStream.write(text.getBytes());
					buffStream.close();
				}
			});

			if (wasError.get()) {
				session.transfer(flowFile, REL_FAILURE);
			} else {

				Map<String, String> mimeAttrs = null;

				if (outputMode.equals(HTML_FORMAT)) {
					mimeAttrs = new HashMap<String, String>() {
						{
							put("mime.type", TEXT_HTML);
							put("orig.mime.type", type.get());
						}
					};
				} else {
					mimeAttrs = new HashMap<String, String>() {
						{
							put("mime.type", TEXT_PLAIN);
							put("orig.mime.type", type.get());
						}
					};
				}

				flowFile = session.putAllAttributes(flowFile, mimeAttrs);
				session.transfer(flowFile, REL_SUCCESS);
			}
			session.commit();
		} catch (final Throwable t) {
			getLogger().error("Unable to process ExtractTextProcessor file " + t.getLocalizedMessage());
			getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, t });
			throw t;
		}
	}
}