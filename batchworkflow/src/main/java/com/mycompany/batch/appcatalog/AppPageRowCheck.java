package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * The test a grid puts every one of its rows through as it fills — {@code STATUS != SUCCESS ||
 * RECORDCOUNT = 0} — and the grammar that sentence is written in.
 *
 * <p>A grid used to say only whether the call behind it worked. That is a different question from
 * whether what came back is <em>right</em>: an endpoint that answers 200 with fifteen rows, three of
 * which reconciled to nothing, is a successful call and a failed run, and the only way to see it was
 * to read the rows. Written on the grid, the same judgement is made once per row as the rows arrive:
 * the ones the expression calls true are drawn in red, and the grid's name carries the tally.
 *
 * <p>Evaluation happens in the browser, where the rows are — see {@code parseRowCheck} and
 * {@code evalRowCheck} in apppage.html, which this mirrors. What happens here is the reading: an
 * expression that does not parse is refused when the page is saved, where the designer can fix it,
 * rather than discovered by an operator wondering why a grid of failures came out entirely green.
 *
 * <h2>The grammar</h2>
 * <pre>
 *   or    := and ( ('||' | 'or') and )*
 *   and   := not ( ('&amp;&amp;' | 'and') not )*
 *   not   := ('!' | 'not') not | cmp
 *   cmp   := term [ op term ]
 *   term  := '(' or ')' | word | 'quoted text'
 *   op    := '=' | '==' | '!=' | '&lt;&gt;' | '&gt;' | '&gt;=' | '&lt;' | '&lt;=' | '~' | '!~'
 * </pre>
 *
 * <p>A bare word is a column of the row where the row has one of that name and the literal text it
 * is otherwise — which is what lets {@code STATUS != SUCCESS} be written the way anyone would say
 * it, with neither side quoted. Quote a value to mean it literally even where a column shares its
 * name.
 */
public final class AppPageRowCheck {

    /** Longest first, so {@code !=} is read as one operator rather than as {@code !} then {@code =}. */
    private static final List<String> COMPARISONS =
            List.of("==", "!=", "<>", ">=", "<=", "!~", "=", ">", "<", "~");

    /** The characters an operator is built out of, and therefore what ends a word. */
    private static final String OPERATOR_CHARS = "=!<>~&|";

    private final List<Token> tokens;
    private int at;

    /** {@code kind} is one of {@code word}, {@code text}, {@code op}, {@code (} or {@code )}. */
    private record Token(String kind, String text) {}

    private AppPageRowCheck(List<Token> tokens) {
        this.tokens = tokens;
    }

    /**
     * Reads the expression, throwing with what is wrong with it when it cannot be read. A blank
     * expression is the ordinary case — most grids have no check — and passes.
     *
     * @throws IllegalArgumentException with a message naming the part that could not be read
     */
    public static void check(String expression) {
        String text = expression == null ? "" : expression.trim();
        if (text.isEmpty()) return;
        AppPageRowCheck parser = new AppPageRowCheck(scan(text));
        parser.or();
        if (parser.at < parser.tokens.size())
            throw new IllegalArgumentException("nothing joins on to '" + parser.tokens.get(parser.at).text() + "'");
    }

    // ── Reading it into tokens ───────────────────────────────────────────────

    private static List<Token> scan(String text) {
        List<Token> out = new ArrayList<>();
        int i = 0;
        while (i < text.length()) {
            char ch = text.charAt(i);
            if (Character.isWhitespace(ch)) { i++; continue; }
            if (ch == '(' || ch == ')') { out.add(new Token(String.valueOf(ch), String.valueOf(ch))); i++; continue; }
            if (ch == '\'' || ch == '"') {
                int end = text.indexOf(ch, i + 1);
                if (end < 0) throw new IllegalArgumentException("a quote is never closed: " + text.substring(i));
                out.add(new Token("text", text.substring(i + 1, end)));
                i = end + 1;
                continue;
            }
            if (OPERATOR_CHARS.indexOf(ch) >= 0) {
                String two = i + 1 < text.length() ? text.substring(i, i + 2) : "";
                if (two.equals("&&") || two.equals("||") || COMPARISONS.contains(two)) {
                    out.add(new Token("op", two));
                    i += 2;
                    continue;
                }
                String one = String.valueOf(ch);
                // "!" on its own is the negation rather than a comparison, so it is let through here
                // even though it is not one of them — "!=" was already taken above.
                if (COMPARISONS.contains(one) || one.equals("!")) {
                    out.add(new Token("op", one));
                    i++;
                    continue;
                }
                throw new IllegalArgumentException("'" + ch + "' is not a test this understands");
            }
            int start = i;
            while (i < text.length() && !Character.isWhitespace(text.charAt(i))
                    && "()'\"".indexOf(text.charAt(i)) < 0 && OPERATOR_CHARS.indexOf(text.charAt(i)) < 0) i++;
            out.add(new Token("word", text.substring(start, i)));
        }
        return out;
    }

    // ── Reading the tokens as a sentence ─────────────────────────────────────

    private void or() {
        and();
        while (isJoin("||", "or")) { at++; and(); }
    }

    private void and() {
        not();
        while (isJoin("&&", "and")) { at++; not(); }
    }

    private void not() {
        if (isOp("!") || isWord("not")) { at++; not(); return; }
        comparison();
    }

    private void comparison() {
        term();
        Token next = peek();
        if (next != null && next.kind().equals("op") && COMPARISONS.contains(next.text())) {
            at++;
            term();
        }
    }

    private void term() {
        Token token = peek();
        if (token == null) throw new IllegalArgumentException("the expression stops before it says anything");
        if (token.kind().equals("(")) {
            at++;
            or();
            if (peek() == null || !peek().kind().equals(")"))
                throw new IllegalArgumentException("a bracket is never closed");
            at++;
            return;
        }
        if (token.kind().equals("word") || token.kind().equals("text")) {
            // A joining word is not a value: "STATUS = or FAILED" is a missing right-hand side
            // rather than a comparison against the word "or".
            if (token.kind().equals("word") && (isWord("or") || isWord("and") || isWord("not")))
                throw new IllegalArgumentException("'" + token.text() + "' joins two tests, so it cannot be one side of one");
            at++;
            return;
        }
        throw new IllegalArgumentException("expected a column or a value, found '" + token.text() + "'");
    }

    private Token peek() {
        return at < tokens.size() ? tokens.get(at) : null;
    }

    private boolean isJoin(String symbol, String word) {
        return isOp(symbol) || isWord(word);
    }

    private boolean isOp(String symbol) {
        Token token = peek();
        return token != null && token.kind().equals("op") && token.text().equals(symbol);
    }

    private boolean isWord(String word) {
        Token token = peek();
        return token != null && token.kind().equals("word") && token.text().equalsIgnoreCase(word);
    }
}
