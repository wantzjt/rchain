package coop.rchain.rholang.interpreter

import coop.rchain.rholang.syntax.rholang_mercury.Absyn.{Ground => AbsynGround, _}
import org.scalatest._

class BoolMatcherSpec extends FlatSpec with Matchers {
  "BoolTrue" should "Compile as GBool(true)" in {
    val btrue = new BoolTrue()

    BoolNormalizeMatcher.normalizeMatch(btrue) should be(GBool(true))
  }
  "BoolFalse" should "Compile as GBool(false)" in {
    val bfalse = new BoolFalse()

    BoolNormalizeMatcher.normalizeMatch(bfalse) should be(GBool(false))
  }
}

class GroundMatcherSpec extends FlatSpec with Matchers {
  "GroundInt" should "Compile as GInt" in {
    val gi = new GroundInt(7)

    GroundNormalizeMatcher.normalizeMatch(gi) should be(GInt(7))
  }
  "GroundString" should "Compile as GString" in {
    val gs = new GroundString("String")

    GroundNormalizeMatcher.normalizeMatch(gs) should be(GString("String"))
  }
  "GroundUri" should "Compile as GUri" in {
    val gu = new GroundUri("Uri")

    GroundNormalizeMatcher.normalizeMatch(gu) should be(GUri("Uri"))
  }
}

class CollectMatcherSpec extends FlatSpec with Matchers {
  val inputs = ProcVisitInputs(
    Par(),
    DebruijnLevelMap[VarSort]().newBindings(List((Some("P"), ProcSort), (Some("x"), NameSort)))._1,
    DebruijnLevelMap[VarSort]())

  "List" should "delegate" in {
    val listData = new ListProc()
    listData.add(new PVar("P"))
    listData.add(new PEval(new NameVar("x")))
    listData.add(new PGround(new GroundInt(7)))
    val list = new PCollect(new CollectList(listData))

    val result = ProcNormalizeMatcher.normalizeMatch(list, inputs)
    result.par should be(
      inputs.par.prepend(
        EList(List[Par](EVar(BoundVar(0)), Eval(ChanVar(BoundVar(1))), GInt(7)), 0)))
    result.knownFree should be(inputs.knownFree)
  }

  "Tuple" should "delegate" in {
    val tupleData = new ListProc()
    tupleData.add(new PVar("Q"))
    tupleData.add(new PEval(new NameVar("y")))
    val tuple = new PCollect(new CollectTuple(tupleData))

    val result = ProcNormalizeMatcher.normalizeMatch(tuple, inputs)
    result.par should be(
      inputs.par.prepend(
        ETuple(List[Par](
                 EVar(FreeVar(0)),
                 Eval(ChanVar(FreeVar(1)))
               ),
               2)))
    result.knownFree should be(
      inputs.knownFree.newBindings(List((Some("Q"), ProcSort), (Some("y"), NameSort)))._1)
  }
  "Tuple" should "propagate free variables" in {
    val tupleData = new ListProc()
    tupleData.add(new PVar("Q"))
    tupleData.add(new PGround(new GroundInt(7)))
    tupleData.add(new PPar(new PGround(new GroundInt(7)), new PVar("Q")))
    val tuple = new PCollect(new CollectTuple(tupleData))

    an[Error] should be thrownBy {
      ProcNormalizeMatcher.normalizeMatch(tuple, inputs)
    }
  }

  "Set" should "delegate" in {
    val setData = new ListProc()
    setData.add(new PAdd(new PVar("P"), new PVar("R")))
    setData.add(new PGround(new GroundInt(7)))
    setData.add(new PPar(new PGround(new GroundInt(8)), new PVar("Q")))
    val set = new PCollect(new CollectSet(setData))

    val result = ProcNormalizeMatcher.normalizeMatch(set, inputs)
    result.par should be(
      inputs.par.prepend(
        ESet(List[Par](EPlus(EVar(BoundVar(0)), EVar(FreeVar(0))),
                       GInt(7),
                       Par(GInt(8)).prepend((EVar(FreeVar(1))))),
             2)))
    result.knownFree should be(
      inputs.knownFree.newBindings(List((Some("R"), ProcSort), (Some("Q"), ProcSort)))._1)
  }

  "Map" should "delegate" in {
    val mapData = new ListKeyValuePair()
    mapData.add(
      new KeyValuePairImpl(new PGround(new GroundInt(7)), new PGround(new GroundString("Seven"))))
    mapData.add(new KeyValuePairImpl(new PVar("P"), new PEval(new NameVar("x"))))
    val map = new PCollect(new CollectMap(mapData))

    val result = ProcNormalizeMatcher.normalizeMatch(map, inputs)
    result.par should be(
      inputs.par.prepend(
        EMap(List[(Par, Par)]((GInt(7), GString("Seven")),
                              (EVar(BoundVar(0)), Eval(ChanVar(BoundVar(1))))),
             0)))
    result.knownFree should be(inputs.knownFree)
  }
}

class ProcMatcherSpec extends FlatSpec with Matchers {
  val inputs = ProcVisitInputs(Par(), DebruijnLevelMap[VarSort](), DebruijnLevelMap[VarSort]())

  "PNil" should "Compile as no modification to the par object" in {
    val nil = new PNil()

    val result = ProcNormalizeMatcher.normalizeMatch(nil, inputs)
    result.par should be(inputs.par)
    result.knownFree should be(inputs.knownFree)
  }

  val pvar = new PVar("x")
  "PVar" should "Compile as BoundVar if it's in env" in {
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), ProcSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pvar, boundInputs)
    result.par should be(inputs.par.prepend(EVar(BoundVar(0))))
    result.knownFree should be(inputs.knownFree)
  }
  "PVar" should "Compile as FreeVar if it's not in env" in {
    val result = ProcNormalizeMatcher.normalizeMatch(pvar, inputs)
    result.par should be(inputs.par.prepend(EVar(FreeVar(0))))
    result.knownFree shouldEqual
      (inputs.knownFree.newBindings(List((Some("x"), ProcSort)))._1)
  }
  "PVar" should "Not compile if it's in env of the wrong sort" in {
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), NameSort)))._1)

    an[Error] should be thrownBy {
      ProcNormalizeMatcher.normalizeMatch(pvar, boundInputs)
    }
  }
  "PVar" should "Not compile if it's used free somewhere else" in {
    val boundInputs =
      inputs.copy(knownFree = inputs.knownFree.newBindings(List((Some("x"), ProcSort)))._1)

    an[Error] should be thrownBy {
      ProcNormalizeMatcher.normalizeMatch(pvar, boundInputs)
    }
  }

  "PEval" should "Handle a bound name varible" in {
    val pEval       = new PEval(new NameVar("x"))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), NameSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pEval, boundInputs)
    result.par should be(inputs.par.prepend(Eval(ChanVar(BoundVar(0)))))
    result.knownFree should be(inputs.knownFree)
  }
  "PEval" should "Collapse a quote" in {
    val pEval       = new PEval(new NameQuote(new PPar(new PVar("x"), new PVar("x"))))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), ProcSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pEval, boundInputs)
    result.par should be(inputs.par.prepend(EVar(BoundVar(0))).prepend(EVar(BoundVar(0))))
    result.knownFree should be(inputs.knownFree)
  }

  "PNot" should "Delegate" in {
    val pNot = new PNot(new PGround(new GroundBool(new BoolFalse())))

    val result = ProcNormalizeMatcher.normalizeMatch(pNot, inputs)
    result.par should be(inputs.par.prepend(ENot(GBool(false))))
    result.knownFree should be(inputs.knownFree)
  }

  "PNeg" should "Delegate" in {
    val pNeg        = new PNeg(new PVar("x"))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), ProcSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pNeg, boundInputs)
    result.par should be(inputs.par.prepend(ENeg(EVar(BoundVar(0)))))
    result.knownFree should be(inputs.knownFree)
  }

  "PMult" should "Delegate" in {
    val pMult       = new PMult(new PVar("x"), new PVar("y"))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), ProcSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pMult, boundInputs)
    result.par should be(inputs.par.prepend(EMult(EVar(BoundVar(0)), EVar(FreeVar(0)))))
    result.knownFree should be(inputs.knownFree.newBindings(List((Some("y"), ProcSort)))._1)
  }

  "PDiv" should "Delegate" in {
    val pDiv = new PDiv(new PGround(new GroundInt(7)), new PGround(new GroundInt(2)))

    val result = ProcNormalizeMatcher.normalizeMatch(pDiv, inputs)
    result.par should be(inputs.par.prepend(EDiv(GInt(7), GInt(2))))
    result.knownFree should be(inputs.knownFree)
  }

  "PAdd" should "Delegate" in {
    val pAdd = new PAdd(new PVar("x"), new PVar("y"))
    val boundInputs = inputs.copy(
      env = inputs.env.newBindings(List((Some("x"), ProcSort), (Some("y"), ProcSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pAdd, boundInputs)
    result.par should be(inputs.par.prepend(EPlus(EVar(BoundVar(0)), EVar(BoundVar(1)))))
    result.knownFree should be(inputs.knownFree)
  }

  "PMinus" should "Delegate" in {
    val pMinus = new PMinus(new PVar("x"), new PMult(new PVar("y"), new PVar("z")))
    val boundInputs = inputs.copy(
      env = inputs.env
        .newBindings(List((Some("x"), ProcSort), (Some("y"), ProcSort), (Some("z"), ProcSort)))
        ._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pMinus, boundInputs)
    result.par should be(
      inputs.par.prepend(EMinus(EVar(BoundVar(0)), EMult(EVar(BoundVar(1)), EVar(BoundVar(2))))))
    result.knownFree should be(inputs.knownFree)
  }

  "PSend" should "handle a basic send" in {
    val sentData = new ListProc()
    sentData.add(new PGround(new GroundInt(7)))
    sentData.add(new PGround(new GroundInt(8)))
    val pSend = new PSend(new NameQuote(new PNil()), new SendSingle(), sentData)

    val result = ProcNormalizeMatcher.normalizeMatch(pSend, inputs)
    result.par should be(
      inputs.par.prepend(Send(Quote(Par()), List[Par](GInt(7), GInt(8)), false, 0)))
    result.knownFree should be(inputs.knownFree)
  }

  "PSend" should "handle a name var" in {
    val sentData = new ListProc()
    sentData.add(new PGround(new GroundInt(7)))
    sentData.add(new PGround(new GroundInt(8)))
    val pSend       = new PSend(new NameVar("x"), new SendSingle(), sentData)
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), NameSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pSend, boundInputs)
    result.par should be(
      inputs.par.prepend(Send(ChanVar(BoundVar(0)), List[Par](GInt(7), GInt(8)), false, 0)))
    result.knownFree should be(inputs.knownFree)
  }

  "PSend" should "propagate knownFree" in {
    val sentData = new ListProc()
    sentData.add(new PGround(new GroundInt(7)))
    sentData.add(new PVar("x"))
    val pSend = new PSend(new NameQuote(new PVar("x")), new SendSingle(), sentData)

    an[Error] should be thrownBy {
      ProcNormalizeMatcher.normalizeMatch(pSend, inputs)
    }
  }

  "PPar" should "Compile both branches into a par object" in {
    val parGround = new PPar(new PGround(new GroundInt(7)), new PGround(new GroundInt(8)))
    val result    = ProcNormalizeMatcher.normalizeMatch(parGround, inputs)
    result.par should be(inputs.par.copy(exprs = List(GInt(8), GInt(7))))
    result.knownFree should be(inputs.knownFree)
  }

  "PPar" should "Compile both branches with the same environment" in {
    val parDoubleBound = new PPar(new PVar("x"), new PVar("x"))
    val boundInputs    = inputs.copy(env = inputs.env.newBindings(List((Some("x"), ProcSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(parDoubleBound, boundInputs)
    result.par should be(inputs.par.copy(exprs = List(EVar(BoundVar(0)), EVar(BoundVar(0)))))
    result.knownFree should be(inputs.knownFree)
  }
  "PPar" should "Not compile if both branches use the same free variable" in {
    val parDoubleFree = new PPar(new PVar("x"), new PVar("x"))
    an[Error] should be thrownBy {
      ProcNormalizeMatcher.normalizeMatch(parDoubleFree, inputs)
    }
  }
  "PPar" should "Accumulate free counts from both branches" in {
    val parDoubleFree = new PPar(new PVar("x"), new PVar("y"))

    val result = ProcNormalizeMatcher.normalizeMatch(parDoubleFree, inputs)
    result.par should be(
      inputs.par.copy(exprs = List(EVar(FreeVar(1)), EVar(FreeVar(0))), freeCount = 2))
    result.knownFree should be(
      inputs.knownFree.newBindings(List((Some("x"), ProcSort), (Some("y"), ProcSort)))._1)
  }

  "PContr" should "Handle a basic contract" in {
    /*  new add in {
          contract add(ret, @x, @y) = {
            ret!(x + y)
          }
        }
        // new is simulated by bindings.
     */
    val listBindings = new ListName()
    listBindings.add(new NameVar("ret"))
    listBindings.add(new NameQuote(new PVar("x")))
    listBindings.add(new NameQuote(new PVar("y")))
    val bindCount = 3
    val freeCount = 0
    val listSend  = new ListProc()
    listSend.add(new PAdd(new PVar("x"), new PVar("y")))
    val pBasicContr = new PContr(new NameVar("add"),
                                 listBindings,
                                 new PSend(new NameVar("ret"), new SendSingle(), listSend))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("add"), NameSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pBasicContr, boundInputs)
    result.par should be(
      inputs.par.prepend(Receive(
        List((List(ChanVar(FreeVar(0)), Quote(EVar(FreeVar(1))), Quote(EVar(FreeVar(2)))),
              ChanVar(BoundVar(0)))),
        Send(ChanVar(BoundVar(1)),
             List[Par](EPlus(EVar(BoundVar(2)), EVar(BoundVar(3)))),
             false,
             0),
        true, // persistent
        bindCount,
        freeCount
      )))
    result.knownFree should be(inputs.knownFree)
  }

  "PContr" should "Not count ground values in the formals towards the bind count" in {
    /*  new ret5 in {
          contract ret5(ret, @5) = {
            ret!(5)
          }
        }
        // new is simulated by bindings.
     */
    val listBindings = new ListName()
    listBindings.add(new NameVar("ret"))
    listBindings.add(new NameQuote(new PGround(new GroundInt(5))))
    val freeCount = 0
    val bindCount = 1
    val listSend  = new ListProc()
    listSend.add(new PGround(new GroundInt(5)))
    val pBasicContr = new PContr(new NameVar("ret5"),
                                 listBindings,
                                 new PSend(new NameVar("ret"), new SendSingle(), listSend))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("ret5"), NameSort)))._1)

    val result = ProcNormalizeMatcher.normalizeMatch(pBasicContr, boundInputs)
    result.par should be(
      inputs.par.copy(receives = List(Receive(
        List((List(ChanVar(FreeVar(0)), Quote(Par().copy(exprs = List(GInt(5))))),
              ChanVar(BoundVar(0)))),
        Par().copy(sends =
          List(Send(ChanVar(BoundVar(1)), List(Par().copy(exprs = List(GInt(5)))), false, 0))),
        true, // persistent
        bindCount,
        freeCount
      ))))
    result.knownFree should be(inputs.knownFree)
  }

  "PInput" should "Handle a simple receive" in {
    // for ( x, y <- @Nil ) { x!(*y) }
    val listBindings = new ListName()
    listBindings.add(new NameVar("x"))
    listBindings.add(new NameVar("y"))
    val listLinearBinds = new ListLinearBind()
    listLinearBinds.add(new LinearBindImpl(listBindings, new NameQuote(new PNil())))
    val linearSimple = new LinearSimple(listLinearBinds)
    val receipt      = new ReceiptLinear(linearSimple)

    val listSend = new ListProc()
    listSend.add(new PEval(new NameVar("y")))
    val body       = new PSend(new NameVar("x"), new SendSingle(), listSend)
    val basicInput = new PInput(receipt, body)
    val bindCount  = 2
    val freeCount  = 0

    val result = ProcNormalizeMatcher.normalizeMatch(basicInput, inputs)
    result.par should be(
      inputs.par.prepend(Receive(
        List((List(ChanVar(FreeVar(0)), ChanVar(FreeVar(1))), Quote(Par()))),
        Send(ChanVar(BoundVar(0)), List[Par](Eval(ChanVar(BoundVar(1)))), false, 0),
        false, // persistent
        bindCount,
        freeCount
      )))
    result.knownFree should be(inputs.knownFree)
  }
  "PInput" should "Handle a more complicated receive" in {
    // for ( (x1, @y1) <- @Nil ; (x2, @y2) <- @1) { x1!(y2) | x2!(y1) }
    val listBindings1 = new ListName()
    listBindings1.add(new NameVar("x1"))
    listBindings1.add(new NameQuote(new PVar("y1")))
    val listBindings2 = new ListName()
    listBindings2.add(new NameVar("x2"))
    listBindings2.add(new NameQuote(new PVar("y2")))
    val listLinearBinds = new ListLinearBind()
    listLinearBinds.add(new LinearBindImpl(listBindings1, new NameQuote(new PNil())))
    listLinearBinds.add(
      new LinearBindImpl(listBindings2, new NameQuote(new PGround(new GroundInt(1)))))
    val linearSimple = new LinearSimple(listLinearBinds)
    val receipt      = new ReceiptLinear(linearSimple)

    val listSend1 = new ListProc()
    listSend1.add(new PVar("y2"))
    val listSend2 = new ListProc()
    listSend2.add(new PVar("y1"))
    val body = new PPar(new PSend(new NameVar("x1"), new SendSingle(), listSend1),
                        new PSend(new NameVar("x2"), new SendSingle(), listSend2))
    val pInput    = new PInput(receipt, body)
    val bindCount = 4
    val freeCount = 0

    val result = ProcNormalizeMatcher.normalizeMatch(pInput, inputs)
    result.par should be(
      inputs.par.prepend(Receive(
        List((List(ChanVar(FreeVar(0)), Quote(EVar(FreeVar(1)))), Quote(Par())),
             (List(ChanVar(FreeVar(0)), Quote(EVar(FreeVar(1)))), Quote(GInt(1)))),
        Par().copy(
          sends = List(Send(ChanVar(BoundVar(2)), List[Par](EVar(BoundVar(1))), false, 0),
                       Send(ChanVar(BoundVar(0)), List[Par](EVar(BoundVar(3))), false, 0))),
        false, // persistent
        bindCount,
        freeCount
      )))
    result.knownFree should be(inputs.knownFree)
  }
  "PInput" should "Fail if a free variable is used in 2 different receives" in {
    // for ( (x1, @y1) <- @Nil ; (x2, @y1) <- @1) { Nil }
    val listBindings1 = new ListName()
    listBindings1.add(new NameVar("x1"))
    listBindings1.add(new NameQuote(new PVar("y1")))
    val listBindings2 = new ListName()
    listBindings2.add(new NameVar("x2"))
    listBindings2.add(new NameQuote(new PVar("y1")))
    val listLinearBinds = new ListLinearBind()
    listLinearBinds.add(new LinearBindImpl(listBindings1, new NameQuote(new PNil())))
    listLinearBinds.add(
      new LinearBindImpl(listBindings2, new NameQuote(new PGround(new GroundInt(1)))))
    val linearSimple = new LinearSimple(listLinearBinds)
    val receipt      = new ReceiptLinear(linearSimple)

    val body   = new PNil()
    val pInput = new PInput(receipt, body)

    an[Error] should be thrownBy {
      ProcNormalizeMatcher.normalizeMatch(pInput, inputs)
    }
  }

  "PNew" should "Bind new variables" in {
    val listNameDecl = new ListNameDecl()
    listNameDecl.add(new NameDeclSimpl("x"))
    listNameDecl.add(new NameDeclSimpl("y"))
    listNameDecl.add(new NameDeclSimpl("z"))
    val listData1 = new ListProc()
    listData1.add(new PGround(new GroundInt(7)))
    val listData2 = new ListProc()
    listData2.add(new PGround(new GroundInt(8)))
    val listData3 = new ListProc()
    listData3.add(new PGround(new GroundInt(9)))
    val pNew = new PNew(
      listNameDecl,
      new PPar(
        new PPar(new PSend(new NameVar("x"), new SendSingle(), listData1),
                 new PSend(new NameVar("y"), new SendSingle(), listData2)),
        new PSend(new NameVar("z"), new SendSingle(), listData3)
      )
    )

    val result = ProcNormalizeMatcher.normalizeMatch(pNew, inputs)
    result.par should be(
      inputs.par.prepend(New(
        3,
        Par(Send(ChanVar(BoundVar(0)), List[Par](GInt(7)), false, 0))
          .prepend(Send(ChanVar(BoundVar(1)), List[Par](GInt(8)), false, 0))
          .prepend(Send(ChanVar(BoundVar(2)), List[Par](GInt(9)), false, 0))
      )))
    result.knownFree should be(inputs.knownFree)
  }

  "PMatch" should "Handle a match inside a for comprehension" in {
    // for (@x <- @Nil) { match x { case 42 => Nil ; case y => Nil } | @Nil!(47)
    val listBindings = new ListName()
    listBindings.add(new NameQuote(new PVar("x")))
    val listLinearBinds = new ListLinearBind()
    listLinearBinds.add(new LinearBindImpl(listBindings, new NameQuote(new PNil())))
    val linearSimple = new LinearSimple(listLinearBinds)
    val receipt      = new ReceiptLinear(linearSimple)

    val listCases = new ListCase()
    listCases.add(new CaseImpl(new PGround(new GroundInt(42)), new PNil()))
    listCases.add(new CaseImpl(new PVar("y"), new PNil()))
    val body = new PMatch(new PVar("x"), listCases)

    val listData = new ListProc()
    listData.add(new PGround(new GroundInt(47)))
    val send47OnNil = new PSend(new NameQuote(new PNil()), new SendSingle(), listData)

    val pPar = new PPar(
      new PInput(receipt, body),
      send47OnNil
    )
    val result    = ProcNormalizeMatcher.normalizeMatch(pPar, inputs)
    val bindCount = 1
    val freeCount = 0

    val expectedResult =
      inputs.par.copy(
        sends = List(Send(Quote(Par()), List[Par](GInt(47)), false, 0)),
        receives = List(
          Receive(
            List((List(Quote(EVar(FreeVar(0)))), Quote(Par()))),
            Match(EVar(BoundVar(0)), List((GInt(42), Par()), (EVar(FreeVar(0)), Par())), 0),
            false,
            bindCount,
            freeCount
          ))
      )
    result.par should be(expectedResult)
    result.knownFree should be(inputs.knownFree)
  }

  "PIf" should "Desugar to match with true/false cases" in {
    // if (true) { @Nil!(47) }
    val condition = new PGround(new GroundBool(new BoolTrue()))
    val listSend  = new ListProc()
    listSend.add(new PGround(new GroundInt(47)))
    val body       = new PSend(new NameQuote(new PNil()), new SendSingle(), listSend)
    val basicInput = new PIf(condition, body)
    val freeCount = 0

    val result = ProcNormalizeMatcher.normalizeMatch(basicInput, inputs)
    result.par should be(
      inputs.par.prepend(
        Match(
          GBool(true),
          List((GBool(true), Send(Quote(Par()), List[Par](GInt(47)), false, 0)),
               (GBool(false), Par())
               // TODO: Fill in type error case
          ),
          freeCount
        )))
    result.knownFree should be(inputs.knownFree)
  }
  "PIfElse" should "Handle a more complicated if statement with an else clause" in {
    // if (47 == 47) { new x in { x!(47) } } else { new y in { y!(47) } }
    val condition = new PEq(new PGround(new GroundInt(47)), new PGround(new GroundInt(47)))
    val xNameDecl = new ListNameDecl()
    xNameDecl.add(new NameDeclSimpl("x"))
    val xSendData = new ListProc()
    xSendData.add(new PGround(new GroundInt(47)))
    val pNewIf = new PNew(
      xNameDecl,
      new PSend(new NameVar("x"), new SendSingle(), xSendData)
    )
    val yNameDecl = new ListNameDecl()
    yNameDecl.add(new NameDeclSimpl("y"))
    val ySendData = new ListProc()
    ySendData.add(new PGround(new GroundInt(47)))
    val pNewElse = new PNew(
      yNameDecl,
      new PSend(new NameVar("y"), new SendSingle(), ySendData)
    )
    val basicInput = new PIfElse(condition, pNewIf, pNewElse)
    val freeCount = 0

    val result = ProcNormalizeMatcher.normalizeMatch(basicInput, inputs)
    result.par should be(
      inputs.par.prepend(Match(
        EEq(GInt(47), GInt(47)),
        List(
          (GBool(true), New(1, Send(ChanVar(BoundVar(0)), List[Par](GInt(47)), false, 0))),
          (GBool(false), New(1, Send(ChanVar(BoundVar(0)), List[Par](GInt(47)), false, 0)))
          // TODO: Fill in type error case
        ),
        freeCount
      )))
    result.knownFree should be(inputs.knownFree)
  }
  "PMatch" should "Fail if a free variable is used twice in the target" in {
    // match 47 { case (y | y) => Nil }
    val listCases = new ListCase()
    listCases.add(new CaseImpl(new PPar(new PVar("y"), new PVar("y")), new PNil()))
    val pMatch = new PMatch(new PGround(new GroundInt(47)), listCases)

    an[Error] should be thrownBy {
      ProcNormalizeMatcher.normalizeMatch(pMatch, inputs)
    }
  }
  "PMatch" should "Handle a match inside a for pattern" in {
    // for (@{match {x | y} { 47 => Nil }} <- @Nil) { Nil }

    val listCases = new ListCase()
    listCases.add(new CaseImpl(new PGround(new GroundInt(47)), new PNil()))
    val pMatch       = new PMatch(new PPar(new PVar("x"), new PVar("y")), listCases)
    val listBindings = new ListName()
    listBindings.add(new NameQuote(pMatch))
    val listLinearBinds = new ListLinearBind()
    listLinearBinds.add(new LinearBindImpl(listBindings, new NameQuote(new PNil())))
    val linearSimple = new LinearSimple(listLinearBinds)
    val receipt      = new ReceiptLinear(linearSimple)
    var input        = new PInput(receipt, new PNil())

    val result    = ProcNormalizeMatcher.normalizeMatch(input, inputs)
    val bindCount = 2
    val freeCount = 0

    val matchTarget = Par(EVar(FreeVar(1))).prepend(EVar(FreeVar(0)))
    val expectedResult =
      inputs.par.copy(
        receives = List(
          Receive(
            List((List(Quote(Match(matchTarget, List((GInt(47), Par())), 2))), Quote(Par()))),
            Par(),
            false,
            bindCount,
            freeCount
          ))
      )
    result.par should be(expectedResult)
    result.knownFree should be(inputs.knownFree)
  }
}

class NameMatcherSpec extends FlatSpec with Matchers {
  val inputs = NameVisitInputs(DebruijnLevelMap[VarSort](), DebruijnLevelMap[VarSort]())

  "NameWildcard" should "Set wildcard flag in knownFree" in {
    val nw     = new NameWildcard()
    val result = NameNormalizeMatcher.normalizeMatch(nw, inputs)
    result.chan should be(ChanVar(FreeVar(0)))
    result.knownFree shouldEqual (inputs.knownFree.setWildcardUsed(1)._1)
  }

  val nvar = new NameVar("x")

  "NameVar" should "Compile as BoundVar if it's in env" in {
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), NameSort)))._1)

    val result = NameNormalizeMatcher.normalizeMatch(nvar, boundInputs)
    result.chan should be(ChanVar(BoundVar(0)))
    result.knownFree should be(inputs.knownFree)
  }
  "NameVar" should "Compile as FreeVar if it's not in env" in {
    val result = NameNormalizeMatcher.normalizeMatch(nvar, inputs)
    result.chan should be(ChanVar(FreeVar(0)))
    result.knownFree shouldEqual
      (inputs.knownFree.newBindings(List((Some("x"), NameSort)))._1)
  }
  "NameVar" should "Not compile if it's in env of the wrong sort" in {
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), ProcSort)))._1)

    an[Error] should be thrownBy {
      NameNormalizeMatcher.normalizeMatch(nvar, boundInputs)
    }
  }
  "NameVar" should "Not compile if it's used free somewhere else" in {
    val boundInputs =
      inputs.copy(knownFree = inputs.knownFree.newBindings(List((Some("x"), NameSort)))._1)

    an[Error] should be thrownBy {
      NameNormalizeMatcher.normalizeMatch(nvar, boundInputs)
    }
  }

  val nqvar = new NameQuote(new PVar("x"))

  "NameQuote" should "compile to a quoted var if the var is bound" in {
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), ProcSort)))._1)
    val nqvar       = new NameQuote(new PVar("x"))
    val result      = NameNormalizeMatcher.normalizeMatch(nqvar, boundInputs)
    result.chan should be(Quote(EVar(BoundVar(0))))
    result.knownFree should be(inputs.knownFree)
  }

  "NameQuote" should "return a free use if the quoted proc has a free var" in {
    val result = NameNormalizeMatcher.normalizeMatch(nqvar, inputs)
    result.chan should be(Quote(Par(EVar(FreeVar(0)))))
    result.knownFree should be(inputs.knownFree.newBindings(List((Some("x"), ProcSort)))._1)
  }

  "NameQuote" should "compile to a quoted ground" in {
    val nqground = new NameQuote(new PGround(new GroundInt(7)))
    val result   = NameNormalizeMatcher.normalizeMatch(nqground, inputs)
    result.chan should be(Quote(GInt(7)))
    result.knownFree should be(inputs.knownFree)
  }

  "NameQuote" should "collapse an eval" in {
    val nqeval      = new NameQuote(new PEval(new NameVar("x")))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), NameSort)))._1)
    val result      = NameNormalizeMatcher.normalizeMatch(nqeval, boundInputs)
    result.chan should be(ChanVar(BoundVar(0)))
    result.knownFree should be(inputs.knownFree)
  }

  "NameQuote" should "not collapse an eval | eval" in {
    val nqeval      = new NameQuote(new PPar(new PEval(new NameVar("x")), new PEval(new NameVar("x"))))
    val boundInputs = inputs.copy(env = inputs.env.newBindings(List((Some("x"), NameSort)))._1)
    val result      = NameNormalizeMatcher.normalizeMatch(nqeval, boundInputs)
    result.chan should be(
      Quote(Par(Eval(ChanVar(BoundVar(0)))).prepend(Eval(ChanVar(BoundVar(0))))))
    result.knownFree should be(inputs.knownFree)
  }
}
