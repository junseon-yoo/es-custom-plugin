package io.scinapse.elasticsearch.export;

import org.elasticsearch.action.ActionType;

public class BulkIdExportAction extends ActionType<BulkIdExportResponse> {

    public static final String NAME = "indices:data/read/bulk_id_export";
    public static final BulkIdExportAction INSTANCE = new BulkIdExportAction();

    private BulkIdExportAction() {
        super(NAME);
    }
}
