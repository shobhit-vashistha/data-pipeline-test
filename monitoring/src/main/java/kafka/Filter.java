package kafka;

public abstract class Filter {

    public static final Filter PASS_ALL = new Filter() {
        @Override
        public boolean passes(String string) {
            return true;
        }
    };

    public abstract boolean passes(String string);

    public static Filter getFilter(String filterString) {
        return filterString == null ? Filter.PASS_ALL : Filter.getContainsFilter(filterString);
    }

    public static Filter getContainsFilter(String matchString) {
        return new Filter() {
            @Override
            public boolean passes(String string) {
                return string.contains(matchString);
            }
        };
    }
}
