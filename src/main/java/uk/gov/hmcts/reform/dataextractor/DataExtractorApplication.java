package uk.gov.hmcts.reform.dataextractor;

@SuppressWarnings({"PMD", "checkstyle:hideutilityclassconstructor"})
public class DataExtractorApplication {

    enum Output {
        CSV("csv"),
        JSON("json"),
        JSON_LINES("json_lines");

        private String label;

        Output(String label) {
            if (label != null) {
                this.label = label.toLowerCase();
            }
        }
    }


    public static void main(String[] args) {

    }

}
