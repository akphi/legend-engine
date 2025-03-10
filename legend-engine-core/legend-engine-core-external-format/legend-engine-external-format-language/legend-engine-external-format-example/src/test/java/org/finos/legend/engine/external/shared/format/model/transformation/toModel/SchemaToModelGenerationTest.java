//  Copyright 2022 Goldman Sachs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package org.finos.legend.engine.external.shared.format.model.transformation.toModel;

import org.finos.legend.engine.external.shared.format.model.ExternalFormatExtension;
import org.finos.legend.engine.external.shared.format.model.ExternalFormatExtensionLoader;
import org.finos.legend.engine.external.shared.format.model.test.ExternalSchemaSetGrammarBuilder;
import org.finos.legend.engine.external.shared.format.model.test.ModelText;
import org.finos.legend.engine.external.shared.format.model.test.ModelTexts;
import org.finos.legend.engine.language.pure.compiler.Compiler;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposer;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposerContext;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.m3.relationship.Association;
import org.finos.legend.engine.protocol.pure.m3.type.Class;
import org.finos.legend.engine.protocol.pure.m3.type.Enumeration;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.externalFormat.ExternalFormatSchemaSet;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaToModelGenerationTest
{
    private static Map<String, ExternalFormatExtension<?>> extensions = ExternalFormatExtensionLoader.extensions();

    public static ExternalSchemaSetGrammarBuilder newExternalSchemaSetGrammarBuilder(String path, String format)
    {
        return new ExternalSchemaSetGrammarBuilder(path, format);
    }

    public static PureModelContextData generateModel(String schemaCode, SchemaToModelConfiguration config)
    {
        return generateModel(schemaCode, config, false, null);
    }

    public static PureModelContextData generateModel(String schemaCode, SchemaToModelConfiguration config, boolean generateBinding, String targetBindingPath)
    {
        PureModelContextData modelData = null;
        PureModel pureModel = null;
        try
        {
            modelData = PureGrammarParser.newInstance().parseModel(schemaCode);
            pureModel = Compiler.compile(modelData, DeploymentMode.TEST, Identity.getAnonymousIdentity().getName());
        }
        catch (Exception e)
        {
            Assert.fail("The schema code cannot be compiled: " + e.getMessage());
        }

        String sourceSchemaSet = modelData.getElementsOfType(ExternalFormatSchemaSet.class).get(0).getPath();
        PureModelContextData generated = new SchemaToModelGenerator(pureModel, "vX_X_X", extensions).generate(config, sourceSchemaSet, generateBinding, targetBindingPath);
        PureModelContextData combined = modelData.combine(generated);
        try
        {
            Compiler.compile(combined, DeploymentMode.TEST, Identity.getAnonymousIdentity().getName());
        }
        catch (Exception e)
        {
            Assert.fail("The generated model cannot be compiled: " + e.getMessage());
        }
        return generated;
    }

    public static ModelTexts modelTextsFromResource(String resourceName)
    {
        return modelTextsFromReader(new InputStreamReader(SchemaToModelGenerationTest.class.getClassLoader().getResourceAsStream(resourceName)));
    }

    public static ModelTexts modelTextsFromString(String text)
    {
        return modelTextsFromReader(new StringReader(text));
    }

    public static ModelTexts modelTextsFromReader(Reader readerIn)
    {
        ModelTexts.Builder builder = ModelTexts.newBuilder();
        try (BufferedReader reader = new BufferedReader(readerIn))
        {
            String path = null;
            StringBuilder grammar = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (line.startsWith(">>>"))
                {
                    if (path != null)
                    {
                        builder.with(new ModelText(path, grammar.toString().trim() + "\n"));
                    }
                    path = line.substring(3);
                    grammar = new StringBuilder();
                }
                else
                {
                    grammar.append(line).append('\n');
                }
            }
            if (path != null)
            {
                builder.with(new ModelText(path, grammar.toString().trim() + "\n"));
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return builder.build();
    }

    public static ModelTexts modelTextsFromContextData(PureModelContextData model)
    {
        ModelTexts.Builder builder = ModelTexts.newBuilder();
        for (PackageableElement element : model.getElements())
        {
            if (element instanceof Class
                    || element instanceof Enumeration
                    || element instanceof Association)
            {
                PureModelContextData subContext = PureModelContextData.newBuilder().withElement(element).build();
                PureGrammarComposer grammarTransformer = PureGrammarComposer.newInstance(PureGrammarComposerContext.Builder.newInstance().build());
                String grammar = grammarTransformer.renderPureModelContextData(subContext);
                builder.with(new ModelText(element.getPath(), grammar));
            }
        }
        return builder.build();
    }

    public static void assertModelTexts(ModelTexts expected, ModelTexts actual)
    {
        Set<String> expectedOnlyPaths = expected.allPaths().stream().filter(p -> !actual.allPaths().contains(p)).collect(Collectors.toSet());
        Set<String> actualOnlyPaths = actual.allPaths().stream().filter(p -> !expected.allPaths().contains(p)).collect(Collectors.toSet());
        Set<String> commonPaths = actual.allPaths().stream().filter(p -> expected.allPaths().contains(p)).collect(Collectors.toSet());

        StringBuilder builder = new StringBuilder();
        if (!expectedOnlyPaths.isEmpty())
        {
            builder.append("The following expected elements are missing\n");
            expectedOnlyPaths.stream().sorted().forEach(p -> builder.append(p).append("\n"));
        }
        if (!actualOnlyPaths.isEmpty())
        {
            builder.append("The following elements were not expected\n");
            actualOnlyPaths.stream().sorted().forEach(p -> builder.append(p).append("\n"));
        }
        for (String path : commonPaths.stream().sorted().collect(Collectors.toList()))
        {
            if (!expected.getText(path).equals(actual.getText(path)))
            {
                builder.append("There are differences for ").append(path).append("\n")
                        .append("Expected:\n").append(expected.getText(path))
                        .append("Actual:\n").append(actual.getText(path));
            }
        }
        Assert.assertTrue(builder.toString(), builder.length() == 0);
    }
}
