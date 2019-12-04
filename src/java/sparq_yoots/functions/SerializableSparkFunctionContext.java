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


    public SerializableSparkFunctionContext(IFn fn) {
	this.fn = fn;
    }

    protected String getSourceName(IFn fn) {
	return fn.getClass().getName();
    }

    protected String parseNamespace(String srcName) {
	return srcName.split("\\$")[0];
    }

    protected void loadNamespace(String namespace) {
	require.invoke(symbol.invoke(namespace));
    }

    protected String name() {
	return this.getClass().getName();
    }

    protected IFn read(ObjectInputStream in) {
	String srcName = "<SRC>";
	String namespace = "<NS>";
	IFn fn = null;
	try {
	    System.out.printf("Deser %s\n", this.name());
	    srcName = (String) in.readObject();
	    namespace = parseNamespace(srcName);

	    System.out.printf("Loading namespace: %s\n", namespace);
	    loadNamespace(namespace);

	    fn = (IFn) in.readObject();
	    return fn;
	} catch (Exception e) {
	    System.out.printf("Deser Error: %s\n", srcName);
	}
	return fn;
    }

    protected void write(ObjectOutputStream out) {
	String srcName = "<SRC>";
	try {
	    System.out.printf("Ser %s\n", this.name());
	    srcName = getSourceName(this.fn);
	    System.out.printf("Serializing %s\n", srcName);

	    out.writeObject(srcName);
	    out.writeObject(this.fn);
	} catch (Exception e) {
	    System.out.printf("Ser Error: %s\n", srcName);
	}
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
	this.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
	this.fn = this.read(in);
    }

    public IFn getFunction() {
	return this.fn;
    }
}
