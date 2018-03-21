package fr.orange.dsti.dsi.dsir.dse.bdt.example.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * This class will raised an input to uppercase
 * @author ZVJN1964
 *
 */
public class CapitalizeUDF extends UDF {
	
	public Text evaluate(Text input)
	{
		return new Text(input.toString().toUpperCase());
	}

}
