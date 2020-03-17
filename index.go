package index

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
)

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//
//  CONSTANTS and PSEUDO-CONSTANTS
//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

const (
	maxKeyLength     = 32
	nullIndexPointer = -1
	//
	decisionNode          = 'D'
	characterNode         = 'X'
	indexKeyNode          = 'R'
	indexTerminalNode     = 'S'
	duplicateKeyNode      = 'K'
	duplicateTerminalNode = 'L'
	//
	left  = 'L'
	right = 'R'
	null  = ' '
)

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//
//   DATA-STRUCTURES
//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

type indexNode struct {
	indexType    byte
	leftPointer  int
	keyCharacter byte
	rightPointer int
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Statistic contains the results of the Statistics scan
//
type Statistic struct {
	Active                     int `json:"Active"`
	Deleted                    int `json:"Deleted"`
	Depth                      int `json:"Depth"`
	DecisionNodeCount          int `json:"DecisionNodes"`
	CharacterNodeCount         int `json:"CharacterNodes"`
	IndexKeyNodeCount          int `json:"IndexKeyNodes"`
	IndexTerminalNodeCount     int `json:"IndexTerminalNodes"`
	DuplicateKeyNodeCount      int `json:"DuplicateKeyNodes"`
	DuplicateTerminalNodeCount int `json:"DuplicateTerminalNodes"`
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Index contains a complete index structure -- one of these is required for each separate index to an array
//
type Index struct {
	indexRootPointer   int
	node               []indexNode
	deletedRootPointer int
	indexMutex         sync.Mutex
	keyCount           int
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//
//   FUNCTIONS
//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

func decimaliseNumber(keyElement int) (keyString string, keyLength int) {
	keyString = strconv.Itoa(keyElement)
	keyLength = len(keyString)
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

func validate(keyInput string) (keyString string, keyLength int) {
	keyInput = strings.TrimSpace(keyInput)
	if len(keyInput) >= maxKeyLength {
		keyString = keyInput[0:maxKeyLength]
	} else {
		keyString = keyInput
	}
	keyLength = len(keyString)
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

func pushStack(inStack []int, indexPointer, inDepth int) (outStack []int, outDepth int) {
	if len(inStack) <= inDepth {
		outStack = append(inStack, indexPointer)
	} else {
		outStack = inStack
		outStack[inDepth] = indexPointer
	}
	outDepth = inDepth + 1
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

func traverseAndCollect(startPointer, endPointer int, indexStructure *Index) (results []int) {
	// collects 'results' of each 'index' node between the start and end pointers
	direction := left
	indexPointer := startPointer
	//
	for indexPointer != endPointer && indexPointer != nullIndexPointer {
		switch indexStructure.node[indexPointer].indexType {
		case indexKeyNode:
			results = append(results, indexStructure.node[indexPointer].leftPointer)
			indexPointer = indexStructure.node[indexPointer].rightPointer
			direction = left
		case indexTerminalNode:
			results = append(results, indexStructure.node[indexPointer].leftPointer)
			indexPointer = indexStructure.node[indexPointer].rightPointer
			direction = right
		case duplicateKeyNode:
			if direction == left {
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
			}
			direction = left
		case duplicateTerminalNode:
			if direction == left {
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
			}
		case decisionNode:
			if direction == left {
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
			}
			direction = left
		case characterNode:
			indexPointer = indexStructure.node[indexPointer].rightPointer
			direction = left
		}
	}
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

func extend(keyString string, keyElement, nextBranchBasePointer int, indexStructure *Index) (extensionPointer int) {
	// creates key nodes in reverse order -- points the leaf node at the next 'branch' sent in as a parameter
	var newIndexNode indexNode
	var newIndexPointer int
	//
	keyLength := len(keyString)
	extensionPointer = nextBranchBasePointer
	//
	for i := keyLength - 1; i >= 0; i-- {
		// create new index node
		byteArray := []byte(keyString[i : i+1])
		newIndexNode.keyCharacter = byteArray[0]
		newIndexNode.rightPointer = extensionPointer
		if i == keyLength-1 { // leaf node
			newIndexNode.indexType = indexTerminalNode
			newIndexNode.leftPointer = keyElement
		} else { // character node
			newIndexNode.indexType = characterNode
			newIndexNode.leftPointer = nullIndexPointer
		}
		//
		if indexStructure.deletedRootPointer == nullIndexPointer { // append to end of indexStructure
			indexStructure.node = append(indexStructure.node, newIndexNode)
			newIndexPointer = len(indexStructure.node) - 1
		} else { // use up one of the 'deleted' nodes
			newIndexPointer = indexStructure.deletedRootPointer
			indexStructure.deletedRootPointer = indexStructure.node[newIndexPointer].rightPointer
			indexStructure.node[newIndexPointer] = newIndexNode
		}
		extensionPointer = newIndexPointer
	}
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//
//   'USER' INTERFACE
//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Initialise sets up an index structure -- must be executed before anything else is performed
//
func Initialise(indexStructure *Index) {
	indexStructure.indexMutex.Lock()
	defer indexStructure.indexMutex.Unlock()
	//
	indexStructure.indexRootPointer = nullIndexPointer
	indexStructure.deletedRootPointer = nullIndexPointer
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Scan returns an array of zero or many Element numbers based on the total content of the supplied index
//      'success' is true if at least one result is found
//
func Scan(indexStructure *Index) (success bool, results []int) {
	indexStructure.indexMutex.Lock()
	defer indexStructure.indexMutex.Unlock()
	//
	results = traverseAndCollect(indexStructure.indexRootPointer, nullIndexPointer, indexStructure)
	success = len(results) > 0
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Search returns an array of zero or many Element numbers based on the input search string
//        a global search is performed -- the input string must be a root of a set of one or more existing keys
//        (a blank, but not null, input is essentially the same as Scan)
//        'success' is true if at least one result is found
//
func Search(keyInput string, indexStructure *Index) (success bool, results []int) {
	indexStructure.indexMutex.Lock()
	defer indexStructure.indexMutex.Unlock()
	//
	var startPointer, endPointer int
	//
	keyString, keyLength := validate(keyInput)
	if keyLength == 0 { // nothing to look for
		return
	}
	//
	indexPointer := indexStructure.indexRootPointer
	endPointer = nullIndexPointer
	i := 0
	for searching := true; searching; {
		if indexPointer == nullIndexPointer { // looked through it all and didn't find it
			return
		}
		switch indexStructure.node[indexPointer].indexType {
		case indexKeyNode, indexTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key
					startPointer = indexPointer
					searching = false // stop looking
				} else { // more characters remain in key
					if indexStructure.node[indexPointer].indexType == indexTerminalNode { // key doesn't match
						return
					} // must have been an 'indexKeyNode' so keep going
					indexPointer = indexStructure.node[indexPointer].rightPointer
					i++
				}
			} else { // key doesn't match
				return
			}
		case decisionNode:
			if keyString[i:i+1] <= string(indexStructure.node[indexPointer].keyCharacter) {
				endPointer = indexPointer // save the 'base' of the left 'branch'
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
			}
		case duplicateKeyNode, duplicateTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key
					startPointer = indexStructure.node[indexPointer].leftPointer // move into the 'duplicates' branch
					searching = false                                            // stop looking
				} else { // more characters remain in key
					if indexStructure.node[indexPointer].indexType == duplicateTerminalNode { // key doesn't match
						return
					} // must have been a 'duplicateKeyNode' so keep going
					indexPointer = indexStructure.node[indexPointer].rightPointer
					i++
				}
			} else { // key doesn't match
				return
			}
		case characterNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key
					startPointer = indexStructure.node[indexPointer].rightPointer
					searching = false // stop looking
				} else { // more characters remain in key so keep going
					indexPointer = indexStructure.node[indexPointer].rightPointer
					i++
				}
			} else { // key doesn't match
				return
			}
		}
	}
	results = traverseAndCollect(startPointer, endPointer, indexStructure)
	success = len(results) > 0
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Select returns an array of zero or many Element numbers based on the input search string
//        a precise search is performed -- the input string must match an existing key -- duplicates are included
//        'success' is true if at least one result is found
//
func Select(keyInput string, indexStructure *Index) (success bool, results []int) {
	indexStructure.indexMutex.Lock()
	defer indexStructure.indexMutex.Unlock()
	//
	keyString, keyLength := validate(keyInput)
	if keyLength == 0 { // nothing to look for
		return
	}
	//
	indexPointer := indexStructure.indexRootPointer
	i := 0
	for {
		if indexPointer == nullIndexPointer { // looked through it all and didn't find it
			return
		}
		switch indexStructure.node[indexPointer].indexType {
		case indexKeyNode, indexTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key
					success = true // found a single match
					results = append(results, indexStructure.node[indexPointer].leftPointer)
					return
				} // otherwise more characters remain in the key
				if indexStructure.node[indexPointer].indexType == indexTerminalNode { // key doesn't match
					return
				} // must have been an 'indexKeyNode' so keep going
				indexPointer = indexStructure.node[indexPointer].rightPointer
				i++
			} else { // key doesn't match
				return
			}
		case decisionNode:
			if keyString[i:i+1] <= string(indexStructure.node[indexPointer].keyCharacter) {
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
			}
		case duplicateKeyNode, duplicateTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key -- base of a 'duplicates' branch
					success = true
					results = traverseAndCollect(indexStructure.node[indexPointer].leftPointer, indexPointer,
						indexStructure)
					success = len(results) > 0
					return
				} // otherwise more characters remain in the key
				if indexStructure.node[indexPointer].indexType == duplicateTerminalNode { // key doesn't match
					return
				} // must have been a 'duplicateKeyNode' so keep going
				indexPointer = indexStructure.node[indexPointer].rightPointer
				i++
			} else { // key doesn't match
				return
			}
		case characterNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key -- but not at a terminal node -- key doesn't match
					return
				}
				indexPointer = indexStructure.node[indexPointer].rightPointer
				i++
			} else { // key doesn't match
				return
			}
		}
	}
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Count returns a count of how many Element numbers (keys) incuding duplicates, are contained in the supplied index
//
func Count(indexStructure *Index) (keyCount int) {
	indexStructure.indexMutex.Lock()
	defer indexStructure.indexMutex.Unlock()
	//
	keyCount = indexStructure.keyCount
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Insert places the input string into the specified index structure along with the supplied "element" number
//
func Insert(keyInput string, keyElement int, indexStructure *Index) (success bool) {
	indexStructure.indexMutex.Lock()
	defer indexStructure.indexMutex.Unlock()
	//
	var decisionIndexPointer, nextBranchBasePointer int
	//
	keyString, keyLength := validate(keyInput)
	if keyLength == 0 { // nothing to look for
		return
	}
	//
	if indexStructure.indexRootPointer == nullIndexPointer { // no index so put the key straight into the structure
		indexStructure.indexRootPointer = extend(keyString, keyElement, nullIndexPointer, indexStructure)
		indexStructure.keyCount++
		success = true
		return
	}
	//
	previousIndexPointer := nullIndexPointer
	indexPointer := indexStructure.indexRootPointer
	duplicateFlag := false
	i := 0
	for searching := true; searching; {
		switch indexStructure.node[indexPointer].indexType {
		case indexKeyNode, indexTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key
					if keyElement == indexStructure.node[indexPointer].leftPointer { // new key EXACTLY same as existing
						return // key value and key element are the same so do nothing
					}
					keyString, keyLength = decimaliseNumber(indexStructure.node[indexPointer].leftPointer)
					linkIndexPointer := // create a duplicate structure and then find where to insert the new key
						extend(keyString, indexStructure.node[indexPointer].leftPointer, indexPointer, indexStructure)
					if indexStructure.node[indexPointer].indexType == indexKeyNode {
						indexStructure.node[indexPointer].indexType = duplicateKeyNode
					} else {
						indexStructure.node[indexPointer].indexType = duplicateTerminalNode // was an indexTerminalNode
					}
					indexStructure.node[indexPointer].leftPointer = linkIndexPointer
					duplicateFlag = true
					i = 0
					keyString, keyLength = decimaliseNumber(keyElement)
					previousIndexPointer = indexPointer
					indexPointer = linkIndexPointer
				} else { // equal so far -- but more characters remain in key
					if indexStructure.node[indexPointer].indexType == indexTerminalNode { // key is a superset
						i++ // at a 'terminal' node -- need to extend the structure
						linkIndexPointer := extend(keyString[i:], keyElement,
							indexStructure.node[indexPointer].rightPointer, indexStructure)
						indexStructure.node[indexPointer].indexType = indexKeyNode
						indexStructure.node[indexPointer].rightPointer = linkIndexPointer
						indexStructure.keyCount++
						success = true
						return
					}
					previousIndexPointer = indexPointer
					indexPointer = indexStructure.node[indexPointer].rightPointer
					i++
				}
			} else { // key doesn't match
				searching = false
				break
			}
		case decisionNode:
			previousIndexPointer = indexPointer
			if keyString[i:i+1] <= string(indexStructure.node[indexPointer].keyCharacter) {
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
			}
		case duplicateKeyNode, duplicateTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key -- need to look through the 'duplicates' branch
					duplicateFlag = true
					keyString, keyLength = decimaliseNumber(keyElement) // start searching the 'duplicates' branch
					i = 0
					previousIndexPointer = indexPointer
					indexPointer = indexStructure.node[indexPointer].leftPointer
				} else { // equal so far -- but more characters remain in key
					if indexStructure.node[indexPointer].indexType == duplicateTerminalNode { // key is a superset
						i++ // at a 'terminal' node -- need to extend the structure
						linkIndexPointer := extend(keyString[i:], keyElement,
							indexStructure.node[indexPointer].rightPointer, indexStructure)
						indexStructure.node[indexPointer].indexType = duplicateKeyNode
						indexStructure.node[indexPointer].rightPointer = linkIndexPointer
						indexStructure.keyCount++
						success = true
						return
					}
					previousIndexPointer = indexPointer
					indexPointer = indexStructure.node[indexPointer].rightPointer
					i++
				}
			} else { // key doesn't match
				searching = false
				break
			}
		case characterNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key -- new key is a subset
					indexStructure.node[indexPointer].indexType = indexKeyNode
					indexStructure.node[indexPointer].leftPointer = keyElement
					indexStructure.keyCount++
					success = true
					return
				} // otherwise keep searching
				previousIndexPointer = indexPointer
				indexPointer = indexStructure.node[indexPointer].rightPointer
				i++
			} else { // key doesn't match
				searching = false
				break
			}
		}
	}
	// new 'branch' required -- needs a 'decisionNode' to be linked into the existing structure
	var newIndexNode indexNode
	newIndexNode.indexType = decisionNode
	if indexStructure.deletedRootPointer == nullIndexPointer { // append new 'decisionNode' to end of index array
		indexStructure.node = append(indexStructure.node, newIndexNode)
		decisionIndexPointer = len(indexStructure.node) - 1
	} else { // use up one of the 'deleted' nodes
		decisionIndexPointer = indexStructure.deletedRootPointer
		indexStructure.deletedRootPointer = indexStructure.node[decisionIndexPointer].rightPointer
		indexStructure.node[decisionIndexPointer] = newIndexNode
	}
	// work out which way to 'attach' the new 'branch' to the 'decisionNode'
	if keyString[i:i+1] > string(indexStructure.node[indexPointer].keyCharacter) {
		threadIndexPointer := indexPointer
		for !(indexStructure.node[threadIndexPointer].indexType == indexTerminalNode ||
			indexStructure.node[threadIndexPointer].indexType == duplicateTerminalNode) {
			threadIndexPointer = indexStructure.node[threadIndexPointer].rightPointer
		}
		nextBranchBasePointer = indexStructure.node[threadIndexPointer].rightPointer
		indexStructure.node[threadIndexPointer].rightPointer = decisionIndexPointer // 'thread' pointer needs reseting
	} else {
		nextBranchBasePointer = decisionIndexPointer
	}
	//
	linkIndexPointer := extend(keyString[i:], keyElement, nextBranchBasePointer, indexStructure)
	// fill in the 'decisionNode'
	if keyString[i:i+1] < string(indexStructure.node[indexPointer].keyCharacter) {
		indexStructure.node[decisionIndexPointer].leftPointer = linkIndexPointer
		byteArray := []byte(keyString[i : i+1])
		indexStructure.node[decisionIndexPointer].keyCharacter = byteArray[0]
		indexStructure.node[decisionIndexPointer].rightPointer = indexPointer
	} else {
		indexStructure.node[decisionIndexPointer].leftPointer = indexPointer
		indexStructure.node[decisionIndexPointer].keyCharacter = indexStructure.node[indexPointer].keyCharacter
		indexStructure.node[decisionIndexPointer].rightPointer = linkIndexPointer
	}
	// link in the 'decisionNode'
	if previousIndexPointer == nullIndexPointer { // if it is at the base of the index then adjust the 'root'
		indexStructure.indexRootPointer = decisionIndexPointer
	} else {
		if indexStructure.node[previousIndexPointer].indexType == decisionNode &&
			keyString[i:i+1] <= string(indexStructure.node[previousIndexPointer].keyCharacter) ||
			indexStructure.node[previousIndexPointer].indexType == duplicateTerminalNode ||
			(indexStructure.node[previousIndexPointer].indexType == duplicateKeyNode && duplicateFlag) {
			indexStructure.node[previousIndexPointer].leftPointer = decisionIndexPointer
		} else {
			indexStructure.node[previousIndexPointer].rightPointer = decisionIndexPointer
		}
	}
	indexStructure.keyCount++
	success = true
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Delete removes a key and its associated "index-number" from the supplied index
//
func Delete(keyInput string, keyElement int, indexStructure *Index) (success bool) {
	indexStructure.indexMutex.Lock()
	defer indexStructure.indexMutex.Unlock()
	//
	keyString, keyLength := validate(keyInput)
	if keyLength == 0 { // nothing to look for
		return
	}
	//
	if indexStructure.indexRootPointer == nullIndexPointer { // no index
		success = false
		return
	}
	//
	previousIndexPointer := nullIndexPointer // keeps track of where the search has been
	deleteIndexPointer := nullIndexPointer   // the base of the branch part that will be deleted
	duplicateNodePointer := nullIndexPointer // the 'duplicate' node if the search goes into a 'duplicates' branch
	linkPreviousPointer := nullIndexPointer  // saves the node before any node to be removed -- so can be re-linked
	//
	indexPointer := indexStructure.indexRootPointer
	deleteBranch := null
	i := 0
	for searching := true; searching; {
		if indexStructure.node[indexPointer].indexType == decisionNode ||
			indexStructure.node[indexPointer].indexType == duplicateKeyNode ||
			indexStructure.node[indexPointer].indexType == indexKeyNode { // branch above may/will be deleted
			deleteIndexPointer = indexPointer
			linkPreviousPointer = previousIndexPointer
		}
		switch indexStructure.node[indexPointer].indexType {
		case indexKeyNode, indexTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key -- key match
					if indexStructure.node[indexPointer].leftPointer != keyElement { // element doesn't
						success = false
						return
					}
					searching = false // found a good match
					break
				} // otherwise more characters remain in the key
				if indexStructure.node[indexPointer].indexType == indexTerminalNode { // key not found
					success = false
					return
				} // must have been an 'indexKeyNode' so keep going
				previousIndexPointer = indexPointer
				indexPointer = indexStructure.node[indexPointer].rightPointer
				i++
			} else { // key not found
				success = false
				return
			}
		case decisionNode:
			previousIndexPointer = indexPointer
			if keyString[i:i+1] <= string(indexStructure.node[indexPointer].keyCharacter) {
				indexPointer = indexStructure.node[indexPointer].leftPointer
				deleteBranch = left // remember which side of a 'decisionNode' (if any) is going to be deleted
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
				deleteBranch = right // remember which side of a 'decisionNode' (if any) is going to be deleted
			}
		case duplicateKeyNode, duplicateTerminalNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key -- base of a 'duplicates' branch -- key match
					duplicateNodePointer = indexPointer // remember the base of the 'duplicates' branch
					i = 0
					keyString, keyLength = decimaliseNumber(keyElement) // create a new search string
					previousIndexPointer = indexPointer
					indexPointer = indexStructure.node[indexPointer].leftPointer // start up the 'duplicates' branch
				} else { // otherwise more characters remain in the key
					if indexStructure.node[indexPointer].indexType == duplicateTerminalNode { // key not found
						success = false
						return
					} // must have been a 'duplicateKeyNode' so keep going
					previousIndexPointer = indexPointer
					indexPointer = indexStructure.node[indexPointer].rightPointer
					i++
				}
			} else { // key not found
				success = false
				return
			}
		case characterNode:
			if keyString[i:i+1] == string(indexStructure.node[indexPointer].keyCharacter) {
				if i+1 == keyLength { // last character in key -- key not found
					success = false
					return
				} // otherwise keep going
				previousIndexPointer = indexPointer
				indexPointer = indexStructure.node[indexPointer].rightPointer
				i++
			} else { // key not found
				success = false
				return
			}
		}
	}
	//
	if duplicateNodePointer != nullIndexPointer { // in a 'duplicates' branch
		elementCount := 0
		direction := left
		scanPointer := indexStructure.node[duplicateNodePointer].leftPointer
		for (scanPointer != duplicateNodePointer && scanPointer != nullIndexPointer) &&
			elementCount < 3 { // at least three so not going to be deleting the 'duplicates' branch
			switch indexStructure.node[scanPointer].indexType { // no 'duplicate*Nodes' in 'duplicates' branch
			case indexKeyNode:
				scanPointer = indexStructure.node[scanPointer].rightPointer
				direction = left
				elementCount++
			case indexTerminalNode:
				scanPointer = indexStructure.node[scanPointer].rightPointer
				direction = right
				elementCount++
			case decisionNode:
				if direction == left {
					scanPointer = indexStructure.node[scanPointer].leftPointer
				} else {
					scanPointer = indexStructure.node[scanPointer].rightPointer
				}
				direction = left
			case characterNode:
				scanPointer = indexStructure.node[scanPointer].rightPointer
				direction = left
			}
		}
		if elementCount < 3 { // going to delete the 'duplicates' branch
			if indexStructure.node[duplicateNodePointer].indexType == duplicateKeyNode { // fix up the type
				indexStructure.node[duplicateNodePointer].indexType = indexKeyNode
			} else {
				indexStructure.node[duplicateNodePointer].indexType = indexTerminalNode
			}
			if indexStructure.node[indexPointer].indexType == indexTerminalNode &&
				indexStructure.node[deleteIndexPointer].indexType == indexKeyNode { // simplest case (1 of 4)
				indexStructure.node[indexPointer].rightPointer = indexStructure.deletedRootPointer
				indexStructure.deletedRootPointer = indexStructure.node[duplicateNodePointer].leftPointer
				indexStructure.node[duplicateNodePointer].leftPointer =
					indexStructure.node[deleteIndexPointer].leftPointer // keep the details of the 'indexKeyNode'
				indexStructure.keyCount--
				success = true
				return
			}
			if indexStructure.node[indexPointer].indexType == indexKeyNode { // next simplest case (2 of 4)
				scanPointer := indexStructure.node[indexPointer].rightPointer
				for indexStructure.node[scanPointer].indexType != indexTerminalNode { // find other 'duplicates' node
					scanPointer = indexStructure.node[scanPointer].rightPointer
				}
				indexStructure.node[scanPointer].rightPointer = indexStructure.deletedRootPointer
				indexStructure.deletedRootPointer = indexStructure.node[duplicateNodePointer].leftPointer
				indexStructure.node[duplicateNodePointer].leftPointer =
					indexStructure.node[scanPointer].leftPointer // keep the details of the 'indexTerminalNode'
				indexStructure.keyCount--
				success = true
				return
			}
			// two 'duplicates' must involve a 'decisionNode' -- left or right case (3 & 4 of 4)
			var scanPointer int
			if deleteBranch == left { // key to be deleted on left so search for other 'duplicates' node on the right
				scanPointer = indexStructure.node[deleteIndexPointer].rightPointer
				for indexStructure.node[scanPointer].indexType != indexTerminalNode { // find other 'duplicates' node
					scanPointer = indexStructure.node[scanPointer].rightPointer
				}
				indexStructure.node[scanPointer].rightPointer = indexStructure.deletedRootPointer
				indexStructure.node[indexPointer].rightPointer = indexStructure.node[duplicateNodePointer].leftPointer
				indexStructure.deletedRootPointer = indexStructure.node[deleteIndexPointer].leftPointer
			} else { // key to be deleted on the right so search for other 'duplicates' node on the left
				scanPointer = indexStructure.node[deleteIndexPointer].leftPointer
				for indexStructure.node[scanPointer].indexType != indexTerminalNode { // find other 'duplicates' node
					scanPointer = indexStructure.node[scanPointer].rightPointer
				}
				indexStructure.node[indexPointer].rightPointer = indexStructure.deletedRootPointer
				indexStructure.node[scanPointer].rightPointer = indexStructure.node[duplicateNodePointer].leftPointer
				indexStructure.deletedRootPointer = indexStructure.node[deleteIndexPointer].leftPointer
			}
			indexStructure.node[duplicateNodePointer].leftPointer =
				indexStructure.node[scanPointer].leftPointer // keep the details of the 'indexTerminalNode'
			indexStructure.keyCount--
			success = true
			return
		}
	}
	// simple subset deletion of an 'indexKeyNode'
	if indexStructure.node[indexPointer].indexType == indexKeyNode {
		indexStructure.node[indexPointer].indexType = characterNode
		indexStructure.node[indexPointer].leftPointer = nullIndexPointer
		indexStructure.keyCount--
		success = true
		return
	}
	// last key in the index
	if deleteIndexPointer == nullIndexPointer {
		indexStructure.node[indexPointer].rightPointer = indexStructure.deletedRootPointer
		indexStructure.deletedRootPointer = indexStructure.indexRootPointer
		indexStructure.indexRootPointer = nullIndexPointer
		indexStructure.keyCount--
		success = true
		return
	}
	// 'deleteIndexPointer' points at either an 'indexKeyNode', a 'duplicateKeyNode' or a 'decisionNode'
	// deleting an 'indexTerminalNode' after an 'indexKeyNode' or 'duplicateKeyNode' with no 'decisionNode' involved
	if indexStructure.node[deleteIndexPointer].indexType == indexKeyNode ||
		indexStructure.node[deleteIndexPointer].indexType == duplicateKeyNode {
		saveIndexPointer := indexStructure.node[deleteIndexPointer].rightPointer
		indexStructure.node[deleteIndexPointer].rightPointer = indexStructure.node[indexPointer].rightPointer
		if indexStructure.node[deleteIndexPointer].indexType == indexKeyNode { // 'indexKeyNode'
			indexStructure.node[deleteIndexPointer].indexType = indexTerminalNode
		} else { // 'duplicateKeyNode'
			indexStructure.node[deleteIndexPointer].indexType = duplicateTerminalNode
		}
		indexStructure.node[indexPointer].rightPointer = indexStructure.deletedRootPointer
		indexStructure.deletedRootPointer = saveIndexPointer
		indexStructure.keyCount--
		success = true
		return
	}
	// 'deleteIndexPointer' must point at a 'decisionNode'
	// deleting a 'decisionNode' and either left or right branch up to the 'indexTerminalNode'
	if deleteBranch == left {
		if linkPreviousPointer != nullIndexPointer { // 'decisionNode' is not at the base of the index
			switch indexStructure.node[linkPreviousPointer].indexType {
			case decisionNode:
				if indexStructure.node[deleteIndexPointer].keyCharacter <=
					indexStructure.node[linkPreviousPointer].keyCharacter {
					indexStructure.node[linkPreviousPointer].leftPointer =
						indexStructure.node[deleteIndexPointer].rightPointer
				} else {
					indexStructure.node[linkPreviousPointer].rightPointer =
						indexStructure.node[deleteIndexPointer].rightPointer
				}
			case duplicateTerminalNode: // in a 'duplicates' branch
				indexStructure.node[linkPreviousPointer].leftPointer =
					indexStructure.node[deleteIndexPointer].rightPointer
			case duplicateKeyNode:
				if duplicateNodePointer != nullIndexPointer { // in a 'duplicates' branch
					indexStructure.node[linkPreviousPointer].leftPointer =
						indexStructure.node[deleteIndexPointer].rightPointer
				} else {
					indexStructure.node[linkPreviousPointer].rightPointer =
						indexStructure.node[deleteIndexPointer].rightPointer
				}
			case characterNode, indexKeyNode:
				indexStructure.node[linkPreviousPointer].rightPointer =
					indexStructure.node[deleteIndexPointer].rightPointer
			}
		} else { // 'decisionNode' is at the base of the index
			indexStructure.indexRootPointer = indexStructure.node[deleteIndexPointer].rightPointer
		}
		indexStructure.node[indexPointer].rightPointer = indexStructure.deletedRootPointer
		indexStructure.deletedRootPointer = deleteIndexPointer
		indexStructure.node[deleteIndexPointer].rightPointer = indexStructure.node[deleteIndexPointer].leftPointer
	} else { // deleteBranch == right
		threadIndexPointer := indexStructure.node[deleteIndexPointer].leftPointer
		for indexStructure.node[threadIndexPointer].rightPointer != deleteIndexPointer { // find the 'threadPointer'
			threadIndexPointer = indexStructure.node[threadIndexPointer].rightPointer
		}
		indexStructure.node[threadIndexPointer].rightPointer = indexStructure.node[indexPointer].rightPointer
		if linkPreviousPointer != nullIndexPointer { // 'decisionNode' is not at the base of the index
			switch indexStructure.node[linkPreviousPointer].indexType {
			case decisionNode:
				if indexStructure.node[deleteIndexPointer].keyCharacter <=
					indexStructure.node[linkPreviousPointer].keyCharacter {
					resetIndex := indexStructure.node[deleteIndexPointer].leftPointer
					if indexStructure.node[resetIndex].indexType != decisionNode {
						indexStructure.node[linkPreviousPointer].keyCharacter =
							indexStructure.node[resetIndex].keyCharacter
					}
					indexStructure.node[linkPreviousPointer].leftPointer =
						indexStructure.node[deleteIndexPointer].leftPointer
				} else {
					indexStructure.node[linkPreviousPointer].rightPointer =
						indexStructure.node[deleteIndexPointer].leftPointer
				}
			case duplicateTerminalNode: // in a 'duplicates' branch
				indexStructure.node[linkPreviousPointer].leftPointer =
					indexStructure.node[deleteIndexPointer].leftPointer
			case duplicateKeyNode:
				if duplicateNodePointer != nullIndexPointer { // in a 'duplicates' branch
					indexStructure.node[linkPreviousPointer].leftPointer =
						indexStructure.node[deleteIndexPointer].leftPointer
				} else {
					indexStructure.node[linkPreviousPointer].rightPointer =
						indexStructure.node[deleteIndexPointer].leftPointer
				}
			case characterNode, indexKeyNode:
				indexStructure.node[linkPreviousPointer].rightPointer =
					indexStructure.node[deleteIndexPointer].leftPointer
			}
		} else { // 'decisionNode' is at the base of the index
			indexStructure.indexRootPointer = indexStructure.node[deleteIndexPointer].leftPointer
		}
		indexStructure.node[indexPointer].rightPointer = indexStructure.deletedRootPointer
		indexStructure.deletedRootPointer = deleteIndexPointer
	}
	indexStructure.keyCount--
	success = true
	return
}

//
/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-/////////-
//

// Statistics scans the specified index structure and returns a structure of counts of the different node types
//            the first parameter returned is an array of numbers -- the second is a json string for human consumption
//
func Statistics(indexStructure *Index) (result Statistic, statsString string) {
	var stack []int
	stackPointer := 0
	direction := left
	result.Active = 0
	result.Deleted = 0
	result.Depth = 0
	result.IndexKeyNodeCount = 0
	result.IndexTerminalNodeCount = 0
	result.CharacterNodeCount = 0
	result.DuplicateKeyNodeCount = 0
	result.DuplicateTerminalNodeCount = 0
	result.DecisionNodeCount = 0
	indexPointer := indexStructure.indexRootPointer
	//
	for scanning := true; scanning; { // start scanning
		if indexPointer == nullIndexPointer { // looked through it all, or nothing there to begin with
			scanning = false
			break
		}
		switch indexStructure.node[indexPointer].indexType {
		//
		case characterNode:
			result.CharacterNodeCount++
			stack, stackPointer = pushStack(stack, indexPointer, stackPointer)
			indexPointer = indexStructure.node[indexPointer].rightPointer
			direction = left
			//
		case indexKeyNode:
			result.IndexKeyNodeCount++
			stack, stackPointer = pushStack(stack, indexPointer, stackPointer)
			indexPointer = indexStructure.node[indexPointer].rightPointer
			direction = left
			//
		case decisionNode:
			if direction == left {
				result.DecisionNodeCount++
				stack, stackPointer = pushStack(stack, indexPointer, stackPointer)
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
				direction = left
			}
			//
		case duplicateKeyNode:
			if direction == left {
				result.DuplicateKeyNodeCount++
				stack, stackPointer = pushStack(stack, indexPointer, stackPointer)
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else {
				indexPointer = indexStructure.node[indexPointer].rightPointer
				direction = left
			}
			//
		case indexTerminalNode:
			result.IndexTerminalNodeCount++
			stack, stackPointer = pushStack(stack, indexPointer, stackPointer)
			indexPointer = indexStructure.node[indexPointer].rightPointer
			if indexPointer != nullIndexPointer { // reset the stack
				for stackPointer = 0; stack[stackPointer] != indexPointer; stackPointer++ {
				}
				stackPointer++
			}
			direction = right
			//
		case duplicateTerminalNode:
			if direction == left {
				result.DuplicateTerminalNodeCount++
				stack, stackPointer = pushStack(stack, indexPointer, stackPointer)
				indexPointer = indexStructure.node[indexPointer].leftPointer
			} else { // going right
				indexPointer = indexStructure.node[indexPointer].rightPointer
				if indexPointer != nullIndexPointer { // reset the stack
					for stackPointer = 0; stack[stackPointer] != indexPointer; stackPointer++ {
					}
					stackPointer++
				}
			}
		}
	} // end scanning
	//
	result.Active = result.IndexKeyNodeCount + result.IndexTerminalNodeCount + result.CharacterNodeCount +
		result.DuplicateKeyNodeCount + result.DuplicateTerminalNodeCount + result.DecisionNodeCount
	result.Depth = len(stack)
	for x := indexStructure.deletedRootPointer; x != nullIndexPointer; x = indexStructure.node[x].rightPointer {
		result.Deleted++
	}
	stats, _ := json.Marshal(result)
	statsString = string(stats)
	return
}
