package rsocket.playground.raft.transport;

import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;

import java.io.*;

public class ObjectPayload {

    public static Payload create(Object data) {
        byte[] dataByte;
        try {
            dataByte = serialize(data);
        } catch (Exception e) {
            throw new ObjectPayloadException(e);
        }
        return DefaultPayload.create(dataByte);
    }

    public static Payload create(Object data, Object metadata) {
        byte[] dataByte;
        byte[] metadataByte;
        try {
            dataByte = serialize(data);
            metadataByte = serialize(metadata);
        } catch (Exception e) {
            throw new ObjectPayloadException(e);
        }
        return DefaultPayload.create(dataByte, metadataByte);
    }

    public static <T> T dataFromPayload(Payload payload, Class<T> type) {
        try {
            return deserialize(payload.getData().array(), type);
        } catch (Exception e) {
            throw new ObjectPayloadException(e);
        }
    }

    public static <T> T metadataFromPayload(Payload payload, Class<T> type) {
        try {
            return deserialize(payload.getMetadata().array(), type);
        } catch (Exception e) {
            throw new ObjectPayloadException(e);
        }
    }

    private static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    private static <T> T deserialize(byte[] data, Class<T> type) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (T) is.readObject();
    }

}
