package fr.orange.dsti.dsi.dsir.dse.bdt.example.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


/**
 * This class must concat all the values received
 * @author ZVJN1964
 * call ... funct(val1, val2, val3 ..., val n)
 *
 */
public class ConcatUDF extends GenericUDF {

	
	//ObjectInspector the received data to a common java type
	private ObjectInspector oi;
	
	private String separator = " ";
	
	//The aim of this function is to control the data receive and check if 
	//they are what we expected 
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		// TODO Auto-generated method stub
		
		int length = arguments.length;
		
		if(length < 2)
		{
			 throw new UDFArgumentLengthException("functions arguments must be at least two");
		}
		
	
		for (ObjectInspector objectInspector : arguments) {
			
			if(objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE)
			{
				throw new UDFArgumentLengthException("functions arguments must be primitive, found : " +objectInspector.getCategory());
			}
		}
		
		//Check if all the data are strings
		for (ObjectInspector objectInspector : arguments) {
			
			if(!(objectInspector  instanceof StringObjectInspector))
			{
				throw new UDFArgumentException("functions arguments must be string, found : " +objectInspector.getTypeName());
			}
		}
		
		

		//The return value must be a string
		oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	    
		
		
		
		return oi;
	}

	
	//The treatment on our data
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
	
		//The returning value
		String result = "";
		
		//perform the concatenation
	    for (int i = 0; i < arguments.length; i++) {
	     if(result.isEmpty())
	     {
	    	 result +=  (((PrimitiveObjectInspector) oi).getPrimitiveJavaObject(arguments[i].get()));
	     }
	     else
	     {
	    	 result +=  separator + (((PrimitiveObjectInspector) oi).getPrimitiveJavaObject(arguments[i].get()));
	     }
	    }
	    
	    //Send the result
	    return result;
	
	}

	@Override
	public String getDisplayString(String[] children) {
		// TODO Auto-generated method stub
		return "concatenation " + children;
	}

}
