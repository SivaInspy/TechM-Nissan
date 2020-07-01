package com.loginController;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;
/**
 *
 * @author ananddw
 *
 */
public class ConvertArray {
    public static void main(String myHelpers[]){
        String jsonArrayString = "{\"fileName\": [{\"name\": \"Anand\",\"last\": \"Dwivedi\",\"place\": \"Bangalore\"}]}";
        JSONObject output;
        try {
            output = new JSONObject(jsonArrayString);
            JSONArray docs = output.getJSONArray("fileName");
            File file=new File("JSONSEPERATOR.csv");
            String csv = CDL.toString(docs);
            FileUtils.writeStringToFile(file, csv);
            System.out.println("Data has been Sucessfully Writeen to "+file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
