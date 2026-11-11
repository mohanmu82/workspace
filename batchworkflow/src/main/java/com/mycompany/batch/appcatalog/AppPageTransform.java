package com.mycompany.batch.appcatalog;

/**
 * One named reshaping step belonging to a page, applied to what an action bound before that result
 * reaches a grid, a dropdown or a text box.
 *
 * <p>Named rather than written on the action itself because the same reshaping is nearly always
 * wanted more than once: the endpoint that returns an auction returns it the same way whichever
 * button asked for it, and the expression that flattens it into rows is one expression however many
 * actions bind it. An action names the ones it wants, in order, through
 * {@link AppPageAction#getTransformNames()}; naming none binds the response exactly as it arrived,
 * which is what every page did before transforms existed.
 *
 * <p>{@link #type} says what the step does. {@link #JSONATA} evaluates {@link #expression} over what
 * it was handed; {@link #XML2JSON} parses an XML document into the object shape the rest of the page
 * already understands and ignores the expression. Having both as steps of one chain is the point:
 * a SOAP endpoint's response is turned into JSON by one step and flattened into rows by the next,
 * and neither step has to know the other exists.
 *
 * <p>Steps run in the browser, against the parsed response body (or, for a metadata action, the flat
 * call record), and the last step's output is what the action's
 * {@link AppPageAction#getArrayPath()} or {@link AppPageAction#getValuePath()} is then read out of.
 * Order matters and is deliberate: the chain reshapes, the path picks out of the reshaped thing.
 */
public class AppPageTransform {

    /** {@link #type}: evaluate {@link #expression} as JSONata over whatever the step was handed. */
    public static final String JSONATA = "JSONATA";

    /**
     * {@link #type}: parse an XML document into JSON — elements become keys, repeats become arrays,
     * attributes land under {@code @attributes} and text under {@code #text}, the same shape the
     * catalog's XML-to-JSON utility produces. Takes the raw response text, so it belongs first in a
     * chain; {@link #expression} is not used.
     */
    public static final String XML2JSON = "XML2JSON";

    /**
     * {@link #type}: apply {@link #expression} as an XSL stylesheet to the XML text it was handed and
     * read what it outputs as CSV — first line the column names, each line after it a row — so an
     * XML response lands in a grid as rows. Takes the raw response text, so it belongs first.
     */
    public static final String XSL2CSV = "XSL2CSV";

    /** How an action names this step. Unique within the page, and required. */
    private String name;
    /** What the step is for, shown next to it in the designer. Free text, optional. */
    private String description;
    /** {@link #JSONATA}, {@link #XML2JSON} or {@link #XSL2CSV}; anything unrecognised reads as JSONATA. */
    private String type = JSONATA;
    /** The JSONata itself, e.g. {@code data.items.{"id": id, "who": owner.name}}; for XSL2CSV, the stylesheet. */
    private String expression;
    /**
     * The name of a {@link JsonataLibraryService shared library} expression to run instead of
     * {@link #expression}. JSONATA only, and the two are alternatives: a step that names one is a
     * reference, and the library's text is what runs, so the same reshaping corrected once is
     * corrected on every page that named it. A step that names none carries its own expression,
     * which is the only thing a step could do before the library existed.
     */
    private String jsonataRef;

    public String getName()               { return name; }
    public void   setName(String name)    { this.name = name == null || name.isBlank() ? null : name.trim(); }

    public String getDescription()                    { return description; }
    public void   setDescription(String description)  { this.description = description; }

    public String getType()              { return type; }
    public void   setType(String type)   {
        this.type = XML2JSON.equalsIgnoreCase(type) ? XML2JSON : XSL2CSV.equalsIgnoreCase(type) ? XSL2CSV : JSONATA;
    }

    public String getExpression()                   { return expression; }
    public void   setExpression(String expression)  { this.expression = expression; }

    public String getJsonataRef()                   { return jsonataRef; }
    public void   setJsonataRef(String jsonataRef)  {
        this.jsonataRef = jsonataRef == null || jsonataRef.isBlank() ? null : jsonataRef.trim();
    }

    /** Whether this step converts XML rather than evaluating an expression. */
    public boolean isXml2Json()   { return XML2JSON.equals(type); }

    /** Whether this step applies an XSL stylesheet and reads its output as CSV rows. */
    public boolean isXsl2Csv()    { return XSL2CSV.equals(type); }

    /** Whether this step's expression comes from the shared library rather than from the page. */
    public boolean isLibraryRef() { return JSONATA.equals(type) && jsonataRef != null; }
}
