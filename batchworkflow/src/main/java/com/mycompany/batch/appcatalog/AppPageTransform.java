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

    /** How an action names this step. Unique within the page, and required. */
    private String name;
    /** What the step is for, shown next to it in the designer. Free text, optional. */
    private String description;
    /** {@link #JSONATA} or {@link #XML2JSON}; anything unrecognised reads as JSONATA. */
    private String type = JSONATA;
    /** The JSONata itself, e.g. {@code data.items.{"id": id, "who": owner.name}}. JSONATA only. */
    private String expression;

    public String getName()               { return name; }
    public void   setName(String name)    { this.name = name == null || name.isBlank() ? null : name.trim(); }

    public String getDescription()                    { return description; }
    public void   setDescription(String description)  { this.description = description; }

    public String getType()              { return type; }
    public void   setType(String type)   { this.type = XML2JSON.equalsIgnoreCase(type) ? XML2JSON : JSONATA; }

    public String getExpression()                   { return expression; }
    public void   setExpression(String expression)  { this.expression = expression; }

    /** Whether this step converts XML rather than evaluating an expression. */
    public boolean isXml2Json()   { return XML2JSON.equals(type); }
}
