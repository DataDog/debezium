/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.relational.TableId;
import io.debezium.util.MockClock;

public class SnapshotMeterTest {

    private SnapshotMeter meter;

    @Before
    public void setUp() {
        meter = new SnapshotMeter(new MockClock());
    }

    @Test
    public void totalRowsToScanIsEmptyOnNewMeter() {
        assertThat(meter.getTotalRowsToScan()).isEmpty();
    }

    @Test
    public void totalRowsToScanTracksEstimatePerTable() {
        TableId table1 = TableId.parse("db.public.orders");
        TableId table2 = TableId.parse("db.public.customers");

        meter.totalRowsToScan(table1, 1000L);
        meter.totalRowsToScan(table2, 500L);

        assertThat(meter.getTotalRowsToScan())
                .containsEntry(table1.toString(), 1000L)
                .containsEntry(table2.toString(), 500L);
    }

    @Test
    public void totalRowsToScanOverwritesExistingEstimateForSameTable() {
        TableId table = TableId.parse("db.public.orders");

        meter.totalRowsToScan(table, 1000L);
        meter.totalRowsToScan(table, 2000L);

        assertThat(meter.getTotalRowsToScan())
                .hasSize(1)
                .containsEntry(table.toString(), 2000L);
    }

    @Test
    public void resetClearsTotalRowsToScan() {
        TableId table = TableId.parse("db.public.orders");
        meter.totalRowsToScan(table, 1000L);

        meter.reset();

        assertThat(meter.getTotalRowsToScan()).isEmpty();
    }

    @Test
    public void resetClearsTotalRowsToScanAlongsideRowsScanned() {
        TableId table = TableId.parse("db.public.orders");
        meter.rowsScanned(table, 500L);
        meter.totalRowsToScan(table, 1000L);

        meter.reset();

        assertThat(meter.getRowsScanned()).isEmpty();
        assertThat(meter.getTotalRowsToScan()).isEmpty();
    }
}
