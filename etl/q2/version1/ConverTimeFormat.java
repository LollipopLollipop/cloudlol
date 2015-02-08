package converttime;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public final class ConverTimeFormat extends UDF{
		  public Text evaluate(final Text s) {
		    if (s == null) { return null; }
		    String[] elements;
		    elements = s.toString().split(" ");
		    String weekDay = elements[0];
		    String month = elements[1].trim();
		    int date = Integer.parseInt(elements[2].trim());
		    String postTime = elements[3].trim();
		    String year = elements[5].trim();
		    String monthDigit = null;
		    switch (month) {
            case "Jan":  
            		monthDigit = "01";
            		break;
            case "Feb":  
            		monthDigit = "02";
                     break;
            case "Mar":  monthDigit = "03";
                     break;
            case "Apr":  monthDigit = "04";
                     break;
            case "May":  monthDigit = "05";
                     break;
            case "Jun":  monthDigit = "06";
                     break;
            case "Jul":  monthDigit = "07";
                     break;
            case "Aug":  monthDigit = "08";
                     break;
            case "Sep":  monthDigit = "09";
                     break;
            case "Oct": monthDigit = "10";
                     break;
            case "Nov": monthDigit = "11";
                     break;
            case "Dec": monthDigit = "12";
                     break;
        }
		    Text res = new Text(year+"-"+monthDigit+"-"+Integer.toString(date)+"+"+postTime);
		    return res;
		  }

}
