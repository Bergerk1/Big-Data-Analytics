package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Task {
    private final String dependentAttribute;
    private final String referencedAttribute;
}
