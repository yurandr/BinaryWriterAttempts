Console.WriteLine("Hello, World!");
var store = new Store();
store.Add(new Store.Record("type1", "path1", "temp_path1"));
store.Add(new Store.Record("type2", "path2", "temp_path2"));
store.Add(new Store.Record("type3", "path3", "temp_path3"));
store.Add(new Store.Record("type4", "path4", "temp_path4"));
store.Add(new Store.Record("type5", "path5", "temp_path5"));

while (store.Read(out var r))
{
    Console.WriteLine(r);
    store.MarkCommandAsProcessed();
}

store.Add(new Store.Record("type6", "path6", "temp_path6"));


public class Store : IDisposable
{
    private readonly int header_size = 256;
    private readonly int record_size = 4096;
    private FileStream _stream;
    private BinaryReader _reader;
    private BinaryWriter _writer;

    private class Header
    {
        public string Caption { get; set; } = "DOCUMENT_STORAGE_SERVER";
        public int CommandsCount { get; set; } = 0;
        public int ProcessedCommandsCount { get; set; } = 0;
        public Header() { }
        public Header(int commandsCount, int processedCommandsCount)
        {
            CommandsCount = commandsCount;
            ProcessedCommandsCount = processedCommandsCount;
        }
        public Header(string caption, int commandsCount, int processedCommandsCount)
            : this(commandsCount, processedCommandsCount)
        {
            if (caption != Caption)
                throw new Exception("Wrong store file format");
        }
    }

    public record Record(string type, string path, string temporary_path);
    public Store()
    {
        _stream = new FileStream("store.dat", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        _reader = new BinaryReader(_stream);
        _writer = new BinaryWriter(_stream);
        if (_stream.Length > _stream.Position)
            ReadHeader();
        else
            WriteHeader(new Header());
    }
    public void Dispose()
    {
        _reader.Close();
        _reader.Dispose();

        _writer.Close();
        _writer.Dispose();

        _stream.Dispose();
    }
    private Header ReadHeader()
    {
        _stream.Seek(0, SeekOrigin.Begin);
        var header_strings = _reader.ReadBytes(header_size).ToStrings(3);
        return new Header(header_strings[0], Int32.Parse(header_strings[1]), Int32.Parse(header_strings[2]));
    }
    private void WriteHeader(Header header)
    {
        _stream.Seek(0, SeekOrigin.Begin);
        var header_to_write = new Header(header.CommandsCount, header.ProcessedCommandsCount);
        if (header.CommandsCount <= header.ProcessedCommandsCount)
            header_to_write = new Header(0, 0);

        _writer.Write(new List<string>() { header_to_write.Caption, header_to_write.CommandsCount.ToString(), header_to_write.ProcessedCommandsCount.ToString() }.ToBuffer(header_size), 0, header_size);
        if (header_to_write.CommandsCount <= header_to_write.ProcessedCommandsCount)
            _stream.SetLength(_stream.Position);
        _writer.Flush();
    }
    public void Add(Record r)
    {
        var header = ReadHeader();
        _stream.Seek(header_size + header.CommandsCount * record_size, SeekOrigin.Begin);
        _writer.Write(new List<string>() { r.type, r.path, r.temporary_path }.ToBuffer(record_size), 0, record_size);
        _writer.Flush();

        header.CommandsCount++;
        WriteHeader(header);
    }
    public bool Read(out Record r)
    {
        r = new Record(string.Empty, string.Empty, string.Empty);
        var header = ReadHeader();
        if (header.CommandsCount <= header.ProcessedCommandsCount)
            return false;

        _stream.Seek(header_size + header.ProcessedCommandsCount * record_size, SeekOrigin.Begin);
        var record_strings = _reader.ReadBytes(record_size).ToStrings(3);
        r = new Record(record_strings[0], record_strings[1], record_strings[2]);
        return true;
    }
    public void MarkCommandAsProcessed()
    {
        var header = ReadHeader();
        header.ProcessedCommandsCount++;
        WriteHeader(header);
    }
};

public static class Ext
{
    public static byte[] ToBuffer(this IEnumerable<string> strings, long buffer_size)
    {
        byte[] buffer = new byte[buffer_size];
        // exception will be triggered if we exceed buffer_size
        using (var ms = new MemoryStream(buffer))
        using (var bw = new BinaryWriter(ms))
        {
            foreach (var s in strings)
                bw.Write(s);
        }
        return buffer;
    }
    public static IList<string> ToStrings(this byte[] data, int numberOfStringToRead)
    {
        using (var ms = new MemoryStream(data))
        using (var br = new BinaryReader(ms))
        {
            var result = new List<string>();
            for (int i=0; i<numberOfStringToRead; ++i)
            {
                result.Add(br.ReadString());
            }
            return result;
        }
    }
}