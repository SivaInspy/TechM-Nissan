import java.nio.file.Files;
import java.nio.file.Paths;

import com.github.opendevl.JFlat;

public class jsontocsv {

    public static void main(String args[]) throws Exception {
        String str = new String(Files.readAllBytes(Paths.get(args[0])));

        JFlat flatMe = new JFlat(str);

        //get the 2D representation of JSON document
        flatMe.json2Sheet().headerSeparator("_").getJsonAsSheet();

        //write the 2D representation in csv format
        flatMe.write2csv("path_to_output"+ args[1]);
    }

}
