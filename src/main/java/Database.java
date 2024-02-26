import java.util.Map;

import static java.util.HashMap.newHashMap;

final class Database {
    private final Map<String, String> simpleKeyValue;

    public Database() {
        this.simpleKeyValue = newHashMap(16);
    }

    public void set(String key, String value) {
        simpleKeyValue.put(key, value);
    }

    public String get(String key) {
        return simpleKeyValue.get(key);
    }
}
