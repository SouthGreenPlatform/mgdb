/**
 * Use this script to delete VariantRunData records that are tied to unexisting projects,
 * an then delete corresponding Variant records if they, as a consequence, have no more VRD
 */

var validProjects = db.projects.distinct("_id");


var variantsWithOrphanVRDs = db.variantRunData.distinct("_id.vi", {"_id.pi" : {$not : {$in:validProjects}}})
shellPrint("Found " + variantsWithOrphanVRDs.length + " variants with orphan VRDs (not in projects " + validProjects + ")");


var wr = db.variantRunData.remove({"_id.pi" : {$not : {$in:validProjects}}});
shellPrint(wr["nRemoved"] + " orphan VRDs were removed");


var orphanVariants = new Array();
db.variants.aggregate([
{$match : {"_id":{$in:variantsWithOrphanVRDs}}}
,{$lookup:
     {
       from: "variantRunData",
       localField: "_id",
       foreignField: "_id.vi",
       as: "vrd"
     }
 }
,{$match : {"vrd":{$size:0}}}
]).forEach(function(record) {
	orphanVariants.push(record["_id"]);
});


wr = db.variants.remove({"_id" : {$in:orphanVariants}});
shellPrint(wr["nRemoved"] + " orphan variants were removed");