package com.joy.example.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.joy.example.udf.CapitalizeUDF;

import junit.framework.Assert;



public class CapitalizeUDFTest  {

	@Test
	public void testUDF() {
		
		//Creation of our udf
		CapitalizeUDF udf = new CapitalizeUDF();
		
		//Test
		Assert.assertEquals("HELLO", udf.evaluate(new Text("Hello")).toString());
		
	}
	
}
