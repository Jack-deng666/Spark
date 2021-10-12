/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/9/14 18:03
 */
public class tst {
    public static void main(String[] args) {
        String str= "null";
        String str1 = chargeValues(str);
        System.out.println(str1);

    }
    public static String chargeValues(String str){
        if (str == null)
            return null;
        else{
            String trim = str.trim();
            if (trim.length() <= 0)
                return null;
            else{
                return trim;
            }
        }
    }
}

