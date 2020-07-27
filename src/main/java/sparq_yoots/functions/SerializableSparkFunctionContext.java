package sparq_yoots.functions;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.IOException;


class SerializableSparkFunctionContext implements Serializable {
    protected IFn fn;

    static final Var require = RT.var("clojure.core", "require");
    static final Var symbol = RT.var("clojure.core", "symbol");

    /**
     * Constructor
     *
     * @param fn UDF source function
     */
    public SerializableSparkFunctionContext(IFn fn) {
	this.fn = fn;
    }

    /**
     * Returns UDF source function
     *
     * @return Clojure function object
     */
    public IFn getFunction() {
	return this.fn;
    }

    /**
     * Returns function fully qualified name.
     *
     * @param fn Source function
     * @return Fully qualified function name
     */
    protected String getSourceName(IFn fn) {
	return fn.getClass().getName();
    }

    /**
     * Parses namespace from fully qualified name
     *
     * @param srcName Fully qualified source name
     * @return namespace value
     */
    protected String parseNamespace(String srcName) {
	return srcName.split("\\$")[0];
    }

    /**
     * Loads namespace
     *
     * @param namespace Source function namespace value
     */
    protected void loadNamespace(String namespace) {
	require.invoke(symbol.invoke(namespace));
    }

    /**
     * Deserializes UDF source function
     *
     * @param in Input stream
     * @return Clojure function object
     */
    protected IFn read(ObjectInputStream in) {
	String srcName = "<SRC>";
	String namespace = "<NS>";
	IFn fn = null;
	try {
	    //System.out.printf("Deser %s\n", this.getClass().getName());
	    srcName = (String) in.readObject();
	    namespace = parseNamespace(srcName);

	    //System.out.printf("Loading namespace: %s\n", namespace);
	    loadNamespace(namespace);

	    fn = (IFn) in.readObject();
	    return fn;
	} catch (Exception e) {
	    System.out.printf("Deser Error: %s\n", srcName);
	}
	return fn;
    }

    /**
     * Serializes source function
     *
     * @param out Output stream
     */
    protected void write(ObjectOutputStream out) {
	String srcName = "<SRC>";
	try {
	    //System.out.printf("Ser %s\n", this.getClass().getName());
	    srcName = getSourceName(this.fn);
	    //System.out.printf("Serializing %s\n", srcName);

	    out.writeObject(srcName);
	    out.writeObject(this.fn);
	} catch (Exception e) {
	    System.out.printf("Ser Error: %s\n", srcName);
	}
    }

    /**
     * Serialization writer
     *
     * @param out Output stream
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
	this.write(out);
    }

    /**
     * Deserialization reader
     *
     * @param in Input stream
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
	this.fn = this.read(in);
    }
}
