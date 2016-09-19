

var node_group= (function() {

    // ---
    // ---
    
    function findSet(node, group_sets){

        var found= -1;
        var set;

        for (var i in group_sets){
            set= group_sets[i];

            //console.log(set.indexOf(node));

            if(set.indexOf(node)> -1){
                found= i;

                break;
            }
        }

        return found;
    }

    // ---

    function xcons(l1, l2){
        
        for(var i in l1){
            if (l2.indexOf(l1[i]) == -1){
                l2.push(l1[i]);
            }
        }
        
        return l2;
    }

    // ---

    function swapIndexForNames(group_sets, nodes){

        return group_sets.map(function(set){
                                return set.map(function(n){
                                                  return nodes[n]["name"]})});
    }

    // ---

    function mkGroupSet(data) {

        var nodes= data.nodes;
        var links= data.links;

        var group_sets= [];
        var entry;
        var source_index;
        var target_index;

        for (var i in links){

            entry= links[i];

           // console.log(entry);

            source_index= findSet(entry.source, group_sets);
            target_index= findSet(entry.target, group_sets);

            if(source_index === -1 && target_index == -1){
                group_sets.push([entry.source, entry.target]);

            } else if(source_index === -1){
                group_sets[target_index].push(entry.source);

            } else if(target_index === -1){
                group_sets[source_index].push(entry.target);

            } else if(source_index === target_index){
                continue; 

            } else {

                xcons(group_sets[source_index], group_sets[target_index]);

                group_sets.splice(source_index, 1);

            }
        }

/*    
        // --- debug & sanity check ---

        console.log("nodes:         "
                    + nodes.length);
        console.log("group members: " 
                    + (group_sets.reduce(function(acc,sum){
                                            return acc+sum.length;},
                                         0)
                      )
                    );
*/
        // ---

        return swapIndexForNames(group_sets, nodes);

    }

    // ---
    // ---
    
    function mkMemberTab(data, gs){

        var mt= {};
        
        for (var i in gs){
            for(var j in gs[i]){
                mt[gs[i][j]]= i;
            } 
        }

        //console.log(JSON.stringify(mt, null, 2));

        return  mt;
    }

    // ---
    
    function setGroupId(id_name, data, gs){

        var mt= mkMemberTab(data, gs);
        var nodes= data.nodes;
        
        for (var i in nodes){
            nodes[i][id_name]= mt[nodes[i].name];
        }
            
        //console.log(JSON.stringify(gs, null, 2));
        
        return data;
    }
    
    // ---
    // ---
    
    return {setGroupId: setGroupId,
            mkGroupSet: mkGroupSet
           };
    
    
}());

console.log("*** " 
            + document.getElementsByTagName('script')[0].baseURI 
            + " loaded: node_group.js" 
            +  " ***"
           );
