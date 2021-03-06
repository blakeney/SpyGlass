package parallelai.spyglass.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class HBaseRecordReaderRegional extends HBaseRecordReaderBase {

	static final Log LOG = LogFactory.getLog(HBaseRecordReaderRegional.class);


    private byte[] nextKey = null;
    private Vector<List<KeyValue>> resultVector = null;
    Map<Long, List<KeyValue>> keyValueMap = null;

    private HBaseTableSplitRegional multiSplit = null;
	private HBaseTableSplitGranular currentSplit = null;

    private HBaseRecordReaderGranular currentRecordReader = null;

	public void init(HBaseTableSplitRegional mSplit) throws IOException {
		multiSplit = mSplit;

		LOG.debug("Creating Multi Split for region location : "
                + multiSplit.getRegionLocation() + " -> " + multiSplit);

        setNextSplit();
	}

	public boolean setNextSplit() throws IOException {
        currentSplit = multiSplit.getNextSplit();

        LOG.debug("IN: setNextSplit : " + currentSplit );

		if( currentSplit != null ) {
			setSplitValue(currentSplit);
			return true;
		} else {
			return false;
		}
	}

    private void setRecordReaderParms(HBaseRecordReaderGranular trr, HBaseTableSplitGranular tSplit) throws IOException {
        HBaseConfigUtils.setRecordReaderParms(trr, tSplit);

        trr.setHTable(htable);
        trr.setInputColumns(trrInputColumns);
        trr.setRowFilter(trrRowFilter);

        trr.init();
    }

	private void setSplitValue(HBaseTableSplitGranular tSplit) throws IOException {
        LOG.debug("IN: setSplitValue : " + tSplit );

        if( currentRecordReader != null ) currentRecordReader.close();

        currentRecordReader = new HBaseRecordReaderGranular();
        setRecordReaderParms(currentRecordReader, currentSplit);
	}

    @Override
    public boolean next(ImmutableBytesWritable ibw, Result result) throws IOException {
        boolean nextFlag = currentRecordReader.next(ibw, result);

        while(nextFlag == false && multiSplit.hasMoreSplits() ) {
            totalPos += currentRecordReader.getPos();
            setNextSplit();
            nextFlag = currentRecordReader.next(ibw, result);
        }

        return nextFlag;
    }

    long totalPos = 0;

    @Override
    public ImmutableBytesWritable createKey() {
        return currentRecordReader.createKey();
    }

    @Override
    public Result createValue() {
        return currentRecordReader.createValue();
    }

    @Override
    public long getPos() throws IOException {
        long pos = totalPos + currentRecordReader.getPos();
        return pos;
    }

    @Override
    public void close() throws IOException {
        currentRecordReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        // ( current count + percent of next count ) / max count
        float prog = ((multiSplit.getCurrSplitCount() + currentRecordReader.getProgress()) / multiSplit.getLength());
        return prog;
    }
}
