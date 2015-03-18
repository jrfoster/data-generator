package org.jrfoster.datagen;

/**
 * This class represents a coded value, like a diagnosis code, or other code
 * and is modeled after the HL7 CX datatype, but missing the 'alternate'
 * related fields.
 * 
 * @author jasonf
 *
 */
public class CodedValue {
    private String identifier;
    private String text;
    private String codingSystem;
    private String codingSystemVersion;
    private String original;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getCodingSystem() {
        return codingSystem;
    }

    public void setCodingSystem(String codingSystem) {
        this.codingSystem = codingSystem;
    }

    public String getCodingSystemVersion() {
        return codingSystemVersion;
    }

    public void setCodingSystemVersion(String codingSystemVersion) {
        this.codingSystemVersion = codingSystemVersion;
    }

    public String getOriginal() {
        return original;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

}
