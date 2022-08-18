import junit.framework.TestCase;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 28.06.13
 * Time: 18:09
 * To change this template use File | Settings | File Templates.
 */
public class JacksonTest extends TestCase {

    public void testJackson() throws IOException {
        List<Map> list = new ArrayList<Map>();
        Map<String, String> map1 = new HashMap<String, String>();
        list.add(map1);
        map1.put("1k", "1v");
        map1.put("2k", "2v");

        Map<String, String> map2 = new HashMap<String, String>();
        list.add(map2);
        map2.put("1k", "1t");
        map2.put("2k", "2t");

        ObjectMapper mapper = new ObjectMapper();
        String string = mapper.writeValueAsString(list);

        Object o = mapper.readValue(string, List.class);

        System.out.println(string);
    }

}
