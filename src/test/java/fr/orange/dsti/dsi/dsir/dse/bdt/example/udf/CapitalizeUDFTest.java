package fr.orange.dsti.dsi.dsir.dse.bdt.example.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

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
