package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter

public class Task {
    private final String dependentAttribute;
    private final String referencedAttribute;
    private final int numUniqueDependent;
    private final int numUniqueReferenced;


    public Task(String dependentAttribute, String referencedAttribute, int numUniqueDependent, int numUniqueReferenced) {
        this.numUniqueDependent = numUniqueDependent;
        this.dependentAttribute = dependentAttribute;
        this.referencedAttribute = referencedAttribute;
        this.numUniqueReferenced = numUniqueReferenced;
    }
    public Task(){
        this.numUniqueDependent = -1;
        this.dependentAttribute = "";
        this.referencedAttribute = "";
        this.numUniqueReferenced = -2;
    }
}
