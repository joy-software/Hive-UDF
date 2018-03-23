package com.joy.example.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import com.joy.example.udf.ConcatUDF;

import junit.framework.Assert;

public class ConcatUDFTest {

	//Only primitive types, list is a complex type
	@Test (expected = UDFArgumentException.class)
	public void testInitialization1() throws UDFArgumentException
	{
		ConcatUDF udf =  new ConcatUDF();
		
		ObjectInspector stringOI =  PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		
		//Creating a list objectInspector of string
		ObjectInspector listOI =  ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
	
		//Test the initialization : must give an error
		ObjectInspector[] ois = new ObjectInspector[2];
		ois[0] = stringOI;
		ois[1] = listOI;
		
		udf.initialize(ois);
	}
	
	//At least two arguments
	@Test (expected = UDFArgumentLengthException.class)
	public void testInitialization2() throws UDFArgumentException
	{
		ConcatUDF udf =  new ConcatUDF();
		
		ObjectInspector stringOI =  PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		
		//Creating a list objectInspector of string
		ObjectInspector listOI =  ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
	
		//Test  the initialization : must give an error
		ObjectInspector[] ois = new ObjectInspector[1];
		ois[0] = stringOI;
		
		udf.initialize(ois);
	}
	
	
	//We must have only StringObjectInspector
	@Test (expected = UDFArgumentException.class)
	public void testInitialization3() throws HiveException
	{
		
		//Setting up our Model
		ConcatUDF udf =  new ConcatUDF();
		
		ObjectInspector stringOI =  PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector doubleOI =  PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
		ObjectInspector[] ois = new ObjectInspector[2];
		ois[0] = stringOI;
		ois[1] = doubleOI;
		
		//Test the initialization: No error 
		//The return value must be a string
		JavaStringObjectInspector result = (JavaStringObjectInspector) udf.initialize(ois);
		
		Object oResult = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("Hello"), new DeferredJavaObject("World")});
	    
		Assert.assertEquals("Hello World", result.getPrimitiveJavaObject(oResult));
		
		
	}
	
	@Test 
	public void testConcatanation() throws HiveException
	{
		
		//Setting up our Model
		ConcatUDF udf =  new ConcatUDF();
		
		ObjectInspector stringOI =  PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector[] ois = new ObjectInspector[2];
		ois[0] = stringOI;
		ois[1] = stringOI;
		
		//Test the initialization: No error 
		//The return value must be a string
		JavaStringObjectInspector result = (JavaStringObjectInspector) udf.initialize(ois);
		
		//Receiving the result Object
		Object oResult = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("Hello"), new DeferredJavaObject("World")});
	    
		//Perform the test
		Assert.assertEquals("Hello World", result.getPrimitiveJavaObject(oResult));
		
		
	}
	
	public static void main(String args[])
	{
		ConcatUDFTest test = new ConcatUDFTest();
		
		try {
			test.testConcatanation();
			//test.testInitialization1();
			//test.testInitialization2();
			
		} catch (UDFArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HiveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
