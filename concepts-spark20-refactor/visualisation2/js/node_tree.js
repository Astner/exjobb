

var node_tree= (function(){
    
// ---
// ---
    
    function getParentNodes(group_set){
        
        var parent_nodes= {};
        var len;
        
        group_set.forEach(function(g, i){
            
                            var len= g.length;
            
                            if(parent_nodes.hasOwnProperty(len)){
                                parent_nodes[len].push({name: "g"+i, children: g});
                            } else {
                                parent_nodes[len]= [{name: "g"+i, children: g}];
                            }
                                
                          }
                         );
        
        return parent_nodes;
    }
    
    // ---
    
    function splitNode(max_len, nodes){
        
        var split= [];
        var node= [];
        var n= 1;
        var m= 1;
        
        for (var i in nodes){

            node.push(nodes[i]);

            m= m + 1;

            if(m > max_len){

                split.push({name: "bin-" + n, children: node});

                n= n + 1;
                m= 1;
                node= [];
            }
        }
        
        if (node.length != 0){
                split.push({name: "bin-" + n, children: node});
        }
        
        return split;
    }
    
    
    function splitNodes(max_len, tree){
        
        var split_nodes= {};
        var nodes;
        
        for (name in tree){
            
            nodes= tree[name];
            
            if (nodes.length > max_len){
                
                split_nodes[name]= splitNode(max_len, nodes);
                
            } else {
        
                split_nodes[name]= nodes;
            }
        }
        
        return split_nodes;
    }
            
    
    //
    // ---
    
    function getChildren(entry_set){
        
        var children= [];
        var sub_tree;
        var n= 0;
        var entry;

        for (var index in entry_set){
            
            entry= entry_set[index];
            
            if (typeof entry == "object"){
                sub_tree= getChildren(entry.children);
                n= n + sub_tree.n;
                children.push({name:     entry.name,
                               children: sub_tree.children,
                               n:        sub_tree.n}
                             );
            } else {
                
                n= n + 1;
                children.push({name:  entry, 
                               value: "leaf",
                               n:     1});
                }
        }
        
        return {children: children, n: n};    
    }
    
    // ---
    
    function getNodeTree(parent_nodes){
        
        var tree= {name: "concepts", children: []};
        var sub_tree;
        var n= 0;
        
        for(var name in parent_nodes){
            
            sub_tree= getChildren(parent_nodes[name]);
            n= n + sub_tree.n;
            
            tree.children.push({name:     "c" + name, 
                                children: sub_tree.children,
                                n:        sub_tree.n });
            
        }
        
        tree.n= n;
        
        return tree;
    }
    
    
    // ---
    
    return {getParentNodes: getParentNodes,
            splitNodes:     splitNodes,
            getNodeTree:    getNodeTree
           };
    
// ---
// ---
    
}());

console.log("*** " 
            + document.getElementsByTagName('script')[0].baseURI 
            + " loaded: node_tree.js" 
            +  " ***");