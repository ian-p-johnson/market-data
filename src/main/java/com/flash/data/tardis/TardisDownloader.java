package com.flash.data.tardis;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.github.luben.zstd.Zstd;
import com.jsoniter.JsonIterator;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.agrona.ExpandableDirectByteBuffer;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.channels.FileChannel;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

@Log4j2
public class TardisDownloader {
    public static final int N_THREADS = 3;

    public static int nThreads = N_THREADS;
    String apiKey;
    boolean dry = false;
    public static final String f2 = "yyyy/MM/dd";
    public static final String fFail = "yyyy-MM-dd_HHmmss";
    //public static final String fFail = "yyyy-MM-dd";
    static ThreadLocal<DateFormat> sdf2 = ThreadLocal.withInitial(() -> new SimpleDateFormat(f2));
    static ThreadLocal<DateFormat> sdfFail = ThreadLocal.withInitial(() -> new SimpleDateFormat(fFail));
    String refDir = "/mnt/z/tardis/datasets";
    String zDir = "/mnt/z/tardis/datasets_zstd";
    String destDir;
    String tmpDir = "/mnt/z/tardis/tmp";
    int verbosity = 0;
    int fail = 1;
    boolean flip;
    ExecutorService executorService;

    boolean verify = true;
    @FunctionalInterface
    interface Operation {
        //void op(String exchangeName, String channel, String symbol, String fromDate, String toDate, String since, String to) throws Exception;
        void op(String exchangeName, String channel, String symbol, Date date, String comment) throws Exception;
    }

    interface Decoder {
        void decode(ByteBuffer bb);
    }
    class Decode_L2 implements Decoder {
        int[] offset = new int[20];
        public void decode(ByteBuffer bb) {
            bb.flip();
            int lineCt = 0, colCt = 1, col = 0, sol = 0;
            while (bb.remaining()>0) {
                byte b = bb.get();
                switch (b) {
                    case ',':
                        offset[colCt++] = col;
                        break;
                    case 10:
                        offset[colCt++] = col;
                        if (lineCt == 0) { // header

                        } else {

                        }
                        lineCt++; colCt = 1; col = 0;
                        sol = bb.position() + 1;
                        break;
                    case 13:
                        break;
                    default:
                        break;
                }
                col++;
            }
            System.out.printf("%,d %s\n", lineCt, bb);
        }
//        exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.4279,2182.41
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42802,3015
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42806,1875.5875319
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42813,2363.23227048
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42816,12809
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42822,14508.3792
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42824,1244.5242865
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42828,13121
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42833,100000
//        bitfinex,XRPUSD,1614384000729000,1614384000736723,true,ask,0.42837,8819

    }
    Decoder decode = new Decode_L2();

    class LoadOperation implements Operation {
        DateFormat sdf = sdf2.get();
        @Override
        public void op(String exchangeName, String channel, String symbol, Date date, String comment) throws Exception {
            loadData(exchangeName, channel, symbol, date, comment);
        }
//        public void op(String exchangeName, String channel, String symbol, String fromDate, String toDate, String since, String to) throws Exception {
//            loadData(exchangeName, channel, symbol.replace('/', '-'),
//                    fromDate.equals("-") ? sdf.parse(since) : sdf.parse(toDate),
//                    (to != null) ? sdf.parse(to) : sdf.parse(toDate));
//        }
    }
    class ScanOperation implements Operation {
        DateFormat sdf = sdf2.get();
        @Override
        public void op(String exchangeName, String channel, String symbol, Date date, String comment) throws Exception {
            scanData(exchangeName, channel, symbol, date, comment);
        }
//        public void op(String exchangeName, String channel, String symbol, String fromDate, String toDate, String since, String to) throws Exception {
//            scanData(exchangeName, channel, symbol.replace('/', '-'),
//                    fromDate.equals("-") ? sdf.parse(since) : sdf.parse(toDate),
//                    (to != null) ? sdf.parse(to) : sdf.parse(toDate));
//        }
    }
    class RenameOperation implements Operation {
        public static final  String MATCH_EXCHANGE = "$e";
        public static final  String MATCH_SYMBOL = "$s";
        public static final  String MATCH_TYPE = "$t";
        public static final  String MATCH_YEAR = "$y";
        public static final  String MATCH_MONTH = "$m";
        public static final  String MATCH_DAY = "$d";
        String fromFormat, toFormat;
        RenameOperation(String fromFormat, String toFormat) {
            this.fromFormat = fromFormat;
            this.toFormat = toFormat;
        }
        @Override
        public void op(String exchangeName, String channel, String symbol, Date date, String comment) throws Exception {
            renameData(exchangeName, channel, symbol, date);
        }
        public static final String rf = "yyyyMMdd";
        static ThreadLocal<DateFormat> trf = ThreadLocal.withInitial(() -> new SimpleDateFormat(rf));
        String replace(String value, String exchange, String type, String symbol, Date date) {
            DateFormat df = trf.get();
            String d = df.format(date);
            return value
                    .replace(MATCH_EXCHANGE, exchange)
                    .replace(MATCH_SYMBOL, symbol)
                    .replace(MATCH_TYPE, type)
                    .replace(MATCH_YEAR, d.substring(0, 4))
                    .replace(MATCH_MONTH, d.substring(4, 6))
                    .replace(MATCH_DAY, d.substring(6, 8))
                    ;
        }
        void renameData(String exchangeName, String type, String symbol, Date date) throws Exception {
            if (destDir == null)
                destDir = refDir + "_zstd";
            val sdf = sdf2.get();

            System.out.printf("");
//            String fromName = replace(fromFormat, exchangeName, type, symbol, date);
//            String toName = replace(toFormat, exchangeName, type, symbol, date);
//            if (dry) {
//
//            } else {
//                File fromFile = new File(fromName);
//                boolean exists = fromFile.exists();
//                if (exists) {
//                    File toFile = new File(toName);
//                    String f = toFile.getParent()+"/";
//                    String a = toFile.getAbsolutePath();
//                    boolean madeDir = new File(toFile.getParent()).mkdirs();
//                    toFile = new File(toName);
//                    boolean renamed = fromFile.renameTo(toFile);
//                    System.out.printf("+rename %s\n  \"%s\"\n  \"%s\"\n", sdf.format(date), fromFile, toFile);
//                }
//            }
            //System.out.printf("-rename %s\n  %s\n  %s\n", sdf.format(date), fromName, toName);

        }
    }

    static PrintWriter failLog;
    static synchronized void fail(String key, String reason) throws IOException {
        if (failLog == null)
            failLog = new PrintWriter(new BufferedWriter(new FileWriter("fail-"+sdfFail.get().format(new Date()), true)));
        failLog.printf("%s,%s\n", key, reason); failLog.flush();
    }
    static Set<String> exceptions = new HashSet<>();
    static void loadFails() throws IOException {
        File curDir = new File(".");
        for(File f : curDir.listFiles( (dir, name) -> name.startsWith("fail-"))) {
            // if(f.isDirectory()) getAllFiles(f);
            if(f.isFile()) {
                System.out.println(f.getName());
                try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
                    String msg;
                    while ((msg = reader.readLine())!= null) {
                        String[] part = msg.split(",");
                        exceptions.add(part[0]);
                    }
                }
            }
        }
        System.out.printf("exceptions: %,d\n", exceptions.size());
        //System.exit(1);
    }
    CronDefinition cronDefinition =
            CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
    CronParser cronParser = new CronParser(cronDefinition);
    Operation op = new LoadOperation();
    public static void main(String[] args) throws Exception {
        System.out.printf("%s\n", Arrays.toString(args));
        TardisDownloader load = new TardisDownloader();
        String exchanges = null, symbols = null, types = null, dateFrom = null, dateTo = null;
        int nThreads = N_THREADS;
        for (int argIx = 0; argIx < args.length; argIx++) {
            switch (args[argIx]) {
                case "-fail":
                    load.fail = Integer.parseInt(args[++argIx]);
                    break;
                case "-flip":
                    load.flip = true;
                    break;
                case "-v":
                    load.verbosity++;
                    break;
                case "-a":
                    load.apiKey = args[++argIx];
                    break;
                case "-s":
                    symbols = args[++argIx];
                    break;
                case "-x":
                    System.exit(0);
                case "-meta":
                    for (String exchange : args[++argIx].split(","))
                        load.loadExchange(Exchange.builder()
                                .name(exchange)
                                .build());
                    break;
                case "-cron":
                    ZonedDateTime now = ZonedDateTime.now();
                    ExecutionTime executionTime = ExecutionTime.forCron(load.cronParser.parse(args[++argIx]));
                    Duration t = executionTime.timeFromLastExecution(now).get();
                    System.out.printf("t = %s\n", t);
                    System.exit(0);
                    break;
                case "-scanDir":
                    Files.list(Path.of(args[++argIx])).forEach((p) -> {
                        //System.out.println(p);;
                        load.scanData(p.toFile());
                    });
                    System.exit(0);

                    break;
                case "-scan":
                    load.op = load.new ScanOperation();
                    break;
                case "-dry":
                    load.dry = true;
                    break;
                case "-regex":
                    int nCnx = 0, nType = 0;
                    var mCnx = new HashMap<String, Integer>();
                    var mType = new HashMap<>();
                    var mSym = new HashMap<String, HashMap>();

// ^([^_]+)_(.+)_(\d{4}-\d{2}-\d{2})_([^_]+)\.csv\.gz$
// ascendex incremental_book_L2 2024-01-20 EXRD-PERP csv.gz
// ^([^_]+)_(.+)_(\d{4})-(\d{2})-(\d{2})_([^_]+)\.csv\.gz$
// ascendex incremental_book_L2 2024 01 20 EXRD-PERP
// -regex /mnt/nvme_4tb/240307-1 "^([^_]+)_(.+)_(\d{4})-(\d{2})-(\d{2})_([^_]+)(\.csv\.gz)$" "%s/%s/%s/%s/%s/%s%s"
// ascendex/incremental_book_L2/2021/03/28/DAFI-USDT.csv.gz
// ascendex/DAFI-USDT/incremental_book_L2/2021/03/28.csv.gz
// "^([^_]+)_(.+)_(\d{4})-(\d{2})-(\d{2})_(.+)(\.csv\.gz)$" "%s/%s/%s/%s/%s/%s%s"
                    String fileName = args[++argIx];
                    String fromRegex = args[++argIx];
                    String format = args[++argIx];
                    format += "\n";
                    Pattern pattern = Pattern.compile(fromRegex);
                    int ct = 0, hit = 0;
                    try (
                            BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
                        String msg = null;
                        Matcher matcher = null;
                        StringBuilder b = new StringBuilder();
                        while ((msg = rdr.readLine()) != null)
                        try {
                            ct++; b.setLength(0);
                            matcher = pattern.matcher(msg);
                            matcher.find();
                            for (int ix = 0; ix < matcher.groupCount(); ix++)
                                b.append("(").append(matcher.group(ix+1)).append(")");
                            if (matcher.groupCount() != 7) throw new RuntimeException();
                            var cnxIx = mCnx.computeIfAbsent(matcher.group(1), (k) -> Integer.valueOf(mCnx.size()+1));
                            var typeIx = mType.computeIfAbsent(matcher.group(2), (k) -> Integer.valueOf(mType.size()+1));
                            var sym = mSym.computeIfAbsent(matcher.group(1), (k) -> new HashMap<>());
                            var symIx = sym.computeIfAbsent(matcher.group(6), (k) -> Integer.valueOf(sym.size()+1));
                            hit++;
                        } catch (Throwable thr) {
                            System.out.printf("%d: %s\n", ct, msg);
                            System.out.printf("%s:%d:%s %s\n", matcher.matches(), matcher.groupCount(), b, msg);
                            System.out.printf(format,
                                    matcher.group(1), matcher.group(2),
                                    matcher.group(3), matcher.group(4), matcher.group(5),
                                    matcher.group(6), matcher.group(7));

                        }
                    }
                    System.out.printf("hit %,d/%,d\ncnx = %s\ntype = %s\n",
                            hit, ct,
                            mCnx, mType);
                    for (var entry: mSym.entrySet())
                        System.out.printf("%,3d: %s\n", entry.getValue().size(), entry.getKey());
                    System.exit(0);
                    break;
                case "-rename":
                    Files.list(Path.of("tmp")).forEach((f) -> {
                        System.out.printf("%s\n", f.getFileName());
                    });
                    String fromFormat = args[++argIx];
                    String toFormat = args[++argIx];
                    load.op = load.new RenameOperation(fromFormat, toFormat);
                    break;
                case "-type":
                    types = args[++argIx];
                    break;
                case "-from":
                    dateFrom = args[++argIx];
                    break;
                case "-to":
                    dateTo = args[++argIx];
                    break;
                case "-ex":
                    exchanges = args[++argIx];
                    break;
                case "-sym":
                    load.listSymbols(exchanges, types);
                    System.exit(0);
                    break;
                case "-tmp":
                    load.tmpDir = args[++argIx];
                    break;
                case "-ref":
                    load.refDir = args[++argIx];
                    break;
                case "-dest":
                    load.destDir = args[++argIx];
                    break;
                case "-t":
                    nThreads = Integer.parseInt(args[++argIx]);
                    break;
            }
        }
        System.out.printf("ex=%s sym=%s tmp=%s ref=%s dest=%s t=%d from=%s to=%s api=%s\n",
                exchanges, symbols, load.tmpDir, load.refDir, load.destDir,
                nThreads, dateFrom, dateTo, load.apiKey);
        int FROM_CURRENT_DAYS = 3;
        int FROM_META_TO_DAYS = 2;
        if (load.fail > 0)
            loadFails();
        new File(load.tmpDir).mkdirs();
        val sdf = sdf2.get();
        load.executorService = Executors.newFixedThreadPool(nThreads);
        for (String exchange : exchanges.split(",")) {
            ExchangeData exchangeData = load.processExchange(exchange);
            System.out.printf("Available Symbols: %d\n", exchangeData.availableSymbols.length);
            Date bufferZone = getBufferDate(Calendar.getInstance(), FROM_CURRENT_DAYS);
            for (String symbol : symbols.split(","))
                for (String type : types.split(",")) {
                    String _symbol = symbol.toUpperCase();
                    var opt = Arrays.stream(exchangeData.availableSymbols).toList().stream().filter(x -> x.id.toUpperCase().equals(_symbol)).findFirst();
                    if (opt.isEmpty()) {
                        System.err.printf("\n%s missing\n", symbol);
                    } else try {
                        AvailableSymbol meta = opt.get();
                        String since = meta.availableSince.substring(0, f2.length()).replace('-', '/');
                        Date _dateFrom = dateFrom.equals("-") ? sdf.parse(since) : sdf.parse(dateFrom);
                        String to = meta.availableTo == null ? null :
                                meta.availableTo.substring(0, f2.length()).replace('-', '/');
                        int BUFFER_FROM_AVAILABLE_TO = 1; // TODO implement me
                        Date _dateToMeta = (to != null) ? sdf.parse(to) : null;
                        //System.out.printf("dateTo %s \n", dateTo);
                        Date _dateToCmd = dateTo.equals("-")
                                ? bufferZone
                                :sdf.parse(dateTo);
                        Date _dateTo = (_dateToMeta != null && _dateToCmd.compareTo(_dateToMeta) > 0)
                                ? getBufferDate(_dateToMeta, FROM_META_TO_DAYS)
                                : _dateToCmd;
                        if (load.verbosity>0)
                            System.out.printf("r:%s->%s c:%s sin:%s %s\n", sdf.format(_dateFrom), sdf.format(_dateTo), sdf.format(_dateToCmd), since, meta);
                        load.forEach(exchange, type, symbol.replace('/', '-'),
                                _dateFrom, _dateTo);
                        //load.op.op(exchange, type, symbol, dateFrom, dateTo, since, to);
                    } catch (Throwable t) {
                        System.err.printf("** dateTo %s\n", dateTo);
                        throw t;
                    }
                }
        }
        System.out.printf("scheduled %,d processed %,d skipped %,d ... waiting to comolete\n",
                    load.ctScheduled, load.ctLoaded, load.ctSkipped);
        load.executorService.shutdown();

        new Thread( () -> {
            // As long as there is activity, wait
            while (System.currentTimeMillis() < (load.lastExecuted + 60*1000*60)) // 60 mins
                sleep(60_000);
            load.executorService.shutdownNow();
        }).start();

        if (!load.executorService.awaitTermination(24, TimeUnit.HOURS))
            load.executorService.shutdownNow();

        out(format("max %,7dK\n", load.max / 1_000));
        System.err.println("complete");
        System.exit(0);

    }

    private static Date getBufferDate(Date from, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(from);
        return getBufferDate(cal, days);
    }
    private static Date getBufferDate(Calendar from, int days) {
        //Calendar today = Calendar.getInstance();
        from.add(Calendar.DATE, -days);
        from.set(Calendar.HOUR_OF_DAY, 0);
        from.set(Calendar.MINUTE, 0);
        Date bufferZone = from.getTime();
        return bufferZone;
    }
    private static Date getAvailableTo() {
        Calendar today = Calendar.getInstance();
        int BUFFER_FROM_CURRENT_DAYS = 3;
        today.add(Calendar.DATE, -BUFFER_FROM_CURRENT_DAYS);
        today.set(Calendar.HOUR_OF_DAY, 0);
        today.set(Calendar.MINUTE, 0);
        Date bufferZone = today.getTime();
        return bufferZone;
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {}
    }
    static interface Loader {
    }

    @ToString
    static class AvailableSymbol {
        String id, type, availableSince, availableTo;
    }

    static class ExchangeData {
        String id, name;
        boolean enabled;
        String availableSince;
        String[] availableChannels;
        AvailableSymbol[] availableSymbols;
    }

    @Builder
    static class Exchange {
        String name;
        Loader loader;
    }

    void listSymbols(String exchanges, String types) throws IOException {
        Set<String> includeTypes = new HashSet<>();
        for (String type: types.split(","))
            includeTypes.add(type);
        for (String exchange : exchanges.split(",")) {
            ExchangeData exchangeData = processExchange(exchange);
            Arrays.sort(exchangeData.availableSymbols, new Comparator<AvailableSymbol>() {
                @Override
                public int compare(AvailableSymbol o1, AvailableSymbol o2) {
                    return o1.id.compareTo(o2.id);
                }
            });
            System.out.printf("Available Symbols: %d\n", exchangeData.availableSymbols.length);
            for (val available: exchangeData.availableSymbols) {
//            switch (available.type) {
//                case "combo":
//                case "option":
//                case "perpetual": // **
//                case "spot":
                //case "future":
//                    break;
//                default:
                //System.out.println(available.id+","+available.type);
                if (includeTypes.size() == 0 || includeTypes.contains(available.type))
                    System.out.println(available.id+",");
            }
        }

    }
    int ctLoaded, ctSkipped, ctScheduled;
    long lastExecuted = 0;
    void forEach(String exchangeName, String channel, String symbol, Date fromDate, Date toDate) throws Exception {
        //log.debug("loadData {}/{} {} {} {}", exchangeName, channel, symbol, sdf.format(fromDate), sdf.format(toDate));
        Calendar cal = Calendar.getInstance();
        cal.setTime(fromDate);
        long ms;
        while ((ms = cal.getTimeInMillis()) <= toDate.getTime()) {
            //System.out.printf("%s\n", sdf2.format(ms));
            Date forTime = cal.getTime();
            executorService.submit(() -> {
                try {
                    val sdf = sdf2.get();
                    lastExecuted = System.currentTimeMillis();
                    op.op(exchangeName, channel, symbol, forTime, "to "+sdf.format(toDate));
                    //loadData(exchangeName, channel, symbol, forTime);
                } catch (Throwable t) {
                    // Eat me
                }
            });
            ctScheduled++;
            cal.add(Calendar.DATE, 1);
        }
    }

//    void scanData(String exchangeName, String channel, String symbol, Date fromDate, Date toDate) throws Exception {
//        val sdf = sdf2.get();
//        log.debug("scanData {}/{} {} {} {}", exchangeName, channel, symbol, sdf.format(fromDate), sdf.format(toDate));
//    }

//    void scanData(String exchangeName, String channel, String symbol, Date date) throws Exception {
//        //System.out.printf("%s %s %s %s\n", exchangeName, channel, symbol, sdf2.format(date));
//        val sdf = sdf2.get();
//        String filename = format("%s_%s_%s_%s.z",
//                exchangeName, channel, sdf.format(date).replace('/', '-'), symbol);
//        String destFile = format("%s/%s", destDir, filename);
//        if (!new File(destFile).exists() && !new File(destFile).exists()) {
//        }
//    }
ExpandableDirectByteBuffer b = new ExpandableDirectByteBuffer(1_000_000_000);
ExpandableDirectByteBuffer d = new ExpandableDirectByteBuffer(Integer.MAX_VALUE);
int max = 0;
void scanData(String exchangeName, String type, String symbol, Date date, String comment) throws Exception {

    if (destDir == null)
        destDir = refDir + "_zstd";
    //System.out.printf("%s %s %s %s\n", exchangeName, channel, symbol, sdf2.format(date));
    val sdf = sdf2.get();
//    String url = format("https://datasets.tardis.dev/v1/%s/%s/%s/%s.csv.gz",
//            exchangeName, channel, sdf.format(date), symbol);
    String filename = format("%s_%s_%s_%s.z",
            exchangeName, type, sdf.format(date).replace('/', '-'), symbol);
//    String tmpFile = format("%s/%s", tmpDir, filename);
//    String refFile = format("%s/%s", refDir, filename);
    String destFile = format("%s/%s", destDir, filename);
    File file = new File(destFile);
    scanData(file);
}

    private void scanData(File file) {
        String destFile = file.getName();
        int read = 0;
        //out(format("%s %s\n", file.exists()?"+":"-", destFile));
        if (file.exists()) try (
            FileInputStream fis = new FileInputStream(file);
            FileChannel channel = fis.getChannel();
        )  {
            ByteBuffer bb = b.byteBuffer();
            ByteBuffer dd = d.byteBuffer();
            bb.clear(); read = channel.read(bb);
            //fis.close(); channel.close();
            //out(format("%,d %s\n", read , destFile));
    //        int size = Zstd.decompress(dd, bb);
            bb.flip();
    //        d.checkLimit(0);
            long fullSize = Zstd.getFrameContentSize(bb);
            dd.clear();
            int size = Zstd.decompress(dd, bb);
            if (size <= 0)
                out(format("*** %,10d %,11d %4.1f%%, %,8d  %s\n", read,  size, size==0?0:(float)read/size*100, fullSize, destFile));
            else {
                //decode.decode(dd);
                max = Math.max(max, size);
                out(format("%,10d %,11d %4.1f%%, %,8d  %s\n", read, size, size == 0 ? 0 : (float) read / size * 100, fullSize, destFile));
            }
        } catch (Throwable t) {
            out(format("%,d %s\n", read , destFile));
            t.printStackTrace();
            System.exit(1);
        }
    }

    void loadData(String exchangeName, String channel, String symbol, Date date, String comment) throws Exception {
    if (destDir == null)
        destDir = refDir + "z";

    //System.out.printf("%s %s %s %s\n", exchangeName, channel, symbol, sdf2.format(date));
    val sdf = sdf2.get();
    String url = format("https://datasets.tardis.dev/v1/%s/%s/%s/%s.csv.gz",
            exchangeName, channel, sdf.format(date), symbol);
    String filename = format("%s_%s_%s_%s.csv.gz",
            exchangeName, channel, sdf.format(date).replace('/', '-'), symbol);
    String tmpFile = format("%s/%s", tmpDir, filename);
    String refFile = format("%s/%s", refDir, filename);
    String destFile = format("%s/%s", destDir, filename);
    String destDir = format("%s", this.destDir);

    //System.out.printf("checking %s\n", filename);
    if (flip ^ exceptions.contains(filename)) {
        if (!flip) out(format("skipping %s\n", filename));
        return;
    }
    if (!new File(refFile).exists() && !new File(destFile).exists()) {
        //out(format("+:%s\n", tmpFile));
        //log.debug("+{}", tmpFile);
        boolean loaded = loadHttpClient(url, tmpFile, filename);
        ctLoaded++;
        if (loaded && verify) {
            int rc = verify(tmpFile);
            if (rc == 0) {
                //System.out.printf("%s -> %s\n", tmpFile,destDir);
                FileUtils.moveFileToDirectory(new File(tmpFile), new File(destDir), false);
            } else
                fail(filename, "ret="+rc);


            //System.out.printf("%s %d\n", tmpFile, rc);
        }
    } else {
        ctSkipped++;
        if (verbosity>1) out(format("-%s\n", filename));
    }
}

    static void out(String msg) {
        System.out.print(msg);
        System.out.flush();
    }

    private static int verify(String fileName) throws Exception {
        //out(format("v:%s", fileName));
        Process p = Runtime.getRuntime().exec(format("gzip -t %s", fileName));
        int ret = p.waitFor();
        if (ret != 0)
            out(format("v:%s = %d\n", fileName, ret));
        return ret;
    }

    public void loadExchange(Exchange exchange) throws IOException {
        loadHttpClient(
                format("https://api.tardis.dev/v1/exchanges/%s", exchange.name),
                format("exchange_%s", exchange.name), "");
    }

    public ExchangeData processExchange(String name) throws IOException {
        ExchangeData data = JsonIterator.deserialize(FileUtils.readFileToString(new File(format("exchange_%s", name))), ExchangeData.class);
        return data;

    }

    public boolean loadHttpClient(String name, String local, String comment) throws IOException {
        HttpClient httpClient = HttpClients.createDefault();
        //out(format("l:%s", local));

        HttpGet httpGet = new HttpGet(name);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey);

        try {
            HttpResponse response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                File file = new File(local);
                byte[] b;
                try (FileOutputStream os = new FileOutputStream(file)) {
                    b = EntityUtils.toByteArray(entity);
                    os.write(b);
                }
                out(format("l:%,6d %s OK %,dM   \r", ctScheduled-ctLoaded-ctSkipped,
                        local, b.length / 1_000_000));
            } else {
                out(format("l:%s %s ERR %s\n", local, comment, response.getStatusLine().getStatusCode()));
                fail(comment, "err="+response.getStatusLine().getStatusCode());
                return false;
                //System.err.println("Failed to download file. Status code: " + response.getStatusLine().getStatusCode());
            }
        } catch (Throwable t) {
            System.err.printf("\n%s\n%s\n", name, t.getMessage());
            fail(comment, "x="+t.getMessage());
            //fail(name, "msg="+t.getMessage());
            t.printStackTrace(System.err);
            return false;
        }
        return true;
    }

}
