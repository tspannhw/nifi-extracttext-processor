/**
 * 
 */
package com.dataflowdeveloper.processors.process;

import java.util.Properties;
import com.google.gson.Gson;
import org.apache.tika.Tika;

/**
 * @author tspann
 *
 */
public class TikaService {

	public String getText(String sentence) {
		if ( sentence == null ) {
			return "";
		}
		String outputJSON = "";
		
		if ( sentence != null) { 
			try {
				String text = "";
				
				outputJSON = new Gson().toJson(text);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return "{\"text\":" + outputJSON + "}";
	}
}