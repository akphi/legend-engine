// Copyright 2021 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.plan.execution.nodes.helpers.freemarker;

import java.util.Arrays;
import java.util.Collections;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.finos.legend.engine.plan.execution.nodes.state.ExecutionState;
import org.finos.legend.engine.plan.execution.result.ConstantResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;

import static org.finos.legend.engine.plan.execution.nodes.helpers.freemarker.FreeMarkerExecutor.overridePropertyForTemplateModel;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.finos.legend.engine.plan.execution.nodes.helpers.freemarker.FreeMarkerExecutor.processRecursively;

public class TestFreeMarkerExecutor
{
    public void processRecursivelyWithFlagSwitching(String input, Map<String, ?> varMap, String templateFunctions, String expected)
    {
        //new flow with overrideTemplateModel
        String result = processRecursively(input, varMap, templateFunctions);
        Assert.assertEquals(expected, result.trim());
        //old flow with freemarker
        System.setProperty(overridePropertyForTemplateModel, "true");
        String result2 = processRecursively(input, varMap, templateFunctions);
        System.clearProperty(overridePropertyForTemplateModel);
        Assert.assertEquals(expected, result2.trim());
    }

    @Test
    public void testCollectionSizeTemplateFunction()
    {
        String query = "final collectionSize :${collectionSize(testCollection)}";

        List smallCollection = Lists.mutable.with(1, 2);
        Map rootMap = new HashMap();
        rootMap.put("testCollection", smallCollection);
        processRecursivelyWithFlagSwitching(query, rootMap, collectionSizeTemplate(), "final collectionSize :2");

        List largeCollection = Lists.mutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622, 623, 624, 625, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652, 653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690, 691, 692, 693, 694, 695, 696, 697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708, 709, 710, 711, 712, 713, 714, 715, 716, 717, 718, 719, 720, 721, 722, 723, 724, 725, 726, 727, 728, 729, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 753, 754, 755, 756, 757, 758, 759, 760, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866, 867, 868, 869, 870, 871, 872, 873, 874, 875, 876, 877, 878, 879, 880, 881, 882, 883, 884, 885, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 899, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920, 921, 922, 923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949, 950, 951, 952, 953, 954, 955, 956, 957, 958, 959, 960, 961, 962, 963, 964, 965, 966, 967, 968, 969, 970, 971, 972, 973, 974, 975, 976, 977, 978, 979, 980, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999, 1000);
        rootMap.put("testCollection", largeCollection);
        processRecursivelyWithFlagSwitching(query, rootMap, collectionSizeTemplate(), "final collectionSize :1000");
    }

    @Test
    public void testFreemarkerSkippedOnEmptyTemplate()
    {
        ExecutionState state = new ExecutionState(Maps.mutable.empty(), Collections.emptyList(), Collections.emptyList(), false, 0);

        String query = "no templates query";
        FreeMarkerExecutor freeMarkerExecutor = Mockito.spy(FreeMarkerExecutor.class);
        Map rootMap = new HashMap();
        String result = FreeMarkerExecutor.process(query, state);
        verify(freeMarkerExecutor, never()).processRecursively(Mockito.anyString(), Mockito.anyMap(), Mockito.anyString());
        Assert.assertEquals(query, result.trim());

    }

    @Test
    public void testParametersProcessedWithNoTemplates()
    {
        Map rootMap = new HashMap();
        rootMap.put("testParam", new ConstantResult("foo"));
        ExecutionState state = new ExecutionState(rootMap, Collections.emptyList(), Collections.emptyList(), false, 0);
        String query = "${testParam}";
        String result = FreeMarkerExecutor.process(query, state);
        Assert.assertEquals("foo", result.trim());
    }

    @Test
    public void testTemplateWithEmbeddedFreeMarker()
    {
        String template =       "<#function withEmbedded>" +
                "<#return  \"${embeddedParam}\"> " +
                "</#function>";
        Map rootMap = new HashMap();
        rootMap.put("embeddedParam", new ConstantResult("embeddedFoo"));
        rootMap.put("outsideParam", new ConstantResult("outsideFoo"));
        List<String> templates = Arrays.asList(template);
        ExecutionState state = new ExecutionState(rootMap, templates, Collections.emptyList(), false, 0);
        String query = "${outsideParam} ${withEmbedded()}";
        String result = FreeMarkerExecutor.process(query, state,template);
        Assert.assertEquals("outsideFoo embeddedFoo", result.trim());
    }

    @Test
    public void testFreemarkerStringWithCombinationsAndSpecialCharacters() throws Exception
    {
        String sql = "this is ${A} and ${B} and ${C} placeholders and ${D} with special characters.";
        Map rootMap = new HashMap<String, String>();
        rootMap.put("A", "A1");
        rootMap.put("B", "${B2}");
        rootMap.put("B2", "B2");
        rootMap.put("C", "${C1}");
        rootMap.put("C1", "${C2}");
        rootMap.put("C2", "${C3}");
        rootMap.put("C3", "${C4}");
        rootMap.put("C4", "C4");
        rootMap.put("D", "abcd<@efg");
        //processing should pass with new flow since it allows placeholder to process only once and avoid this issue.
        Assert.assertEquals("this is A1 and B2 and C4 placeholders and abcd<@efg with special characters.", processRecursively(sql, rootMap, ""));
    }

    @Test
    public void testFreemarkerCharacterFail() throws Exception
    {
        //case: instances found for ${} where we dont pass in variables in varMap
        //outcome: should fail since the user should not be setting column names with freemarker template
        String sqlQuery3 = "select \"root\".countrycode as \"${countryCode}\" , where (\"root\".NAME = '${firstName?replace(\"'\", \"''\")}'), \"root\".countryCodeZip as \"${countryCodeZip}\" from jasamk_test_data.country_schema as \"root\" ";
        Map rootMap3 = new HashMap<String, String>();
        rootMap3.put("countryCodeZip", "75201");
        Assert.assertThrows(RuntimeException.class, () -> processRecursivelyWithFlagSwitching(sqlQuery3, rootMap3, "", ""));
    }

    @Test
    public void testFreemarkerCharacterFail2() throws Exception
    {
        //case4: instance where ${.. , is unclosed
        //outcome: expected to fail because this violates freemarker template rules . This will fail before you process it
        String sqlQuery4 = "select \"root\".countrycode as \"${countryCode\" , \"root\".countryCodeZip as \"countryCodeZip}\" from test_schema as \"root\" ";
        Assert.assertThrows(RuntimeException.class, () -> processRecursivelyWithFlagSwitching(sqlQuery4, new HashMap<String, String>(), "", ""));
    }

    @Test
    public void testFreemarkerCharacterFail3() throws Exception
    {
        //case 5: adding '<@' to the string, should throw error because freemarker prevents use of < within an expression
        //outcome: should fail as it doesnt comply with freemaker template rules
        String sqlQuery5 = "'${firstname<@}";
        Map rootMap5 = new HashMap<String, String>();
        rootMap5.put("firstname", "test");
        Assert.assertThrows("SEVERE: Error executing FreeMarker template", RuntimeException.class, () -> processRecursivelyWithFlagSwitching(sqlQuery5, rootMap5, "", ""));
    }

    @Test
    public void testFreemarkerCharacterFail4() throws Exception
    {
        //case6: have a tailing ${ with other logic and end bracket in the end  in a join logic
        //outcome: should fail, users shouldnt be allowed to set their variables as ${countryName and countryCode} as they are mimicking freemarker template
        String sqlQuery6 = "select \"root\".countryname as \"${countryName\", listagg(\"root\".countrycode, ';') as \"countryCode}\" from test_schema as \"root\" group by \"${countryName\"";
        Assert.assertThrows("Caused by: freemarker.core.ParseException: Syntax error in template \"template\" in line 1, column 44:\n" +
                "Encountered \"\\\", listagg(\\\"\", but was expecting one of:", RuntimeException.class, () -> processRecursivelyWithFlagSwitching(sqlQuery6, new HashMap<String, String>(), "", ""));
    }

    public static String collectionSizeTemplate()
    {
        return "<#function collectionSize collection>" +
                "<#return collection?size> " +
                "</#function>";
    }
}
