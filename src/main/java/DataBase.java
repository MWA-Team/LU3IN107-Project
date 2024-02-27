import java.util.HashMap;
import java.util.Map;

public class DataBase<T> {
    private Map<Integer,T> dataMap = new HashMap<>();

    public void addData(Integer id,T any){
        dataMap.put(id, any);
    }

    public void removeData(Integer id,T any){
         dataMap.remove(id, any);
    }    

    public T getData(Integer id){
        return dataMap.get(id);
    }

    public Map<Integer,T> getAllData(Integer id,T any){
        return new HashMap<>(dataMap);
    }
}
